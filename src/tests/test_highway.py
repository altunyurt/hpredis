"""Highway mode tests: one offsets table plus the arena, read natively.

The table is n*24 bytes of little-endian int64 triples (offset, length,
resp_type) with offsets into the arena bytearray.
"""

import struct
import time
import tracemalloc

import numpy as np
import pytest

import hpredis

DIM = 1536
PAYLOAD = struct.pack(f"{DIM}f", *[float(i) for i in range(DIM)])


def _bulk(payload: bytes) -> bytes:
    return b"$%d\r\n" % len(payload) + payload + b"\r\n"


def _slices(table):
    return [struct.unpack_from("<qqq", table, i) for i in range(0, len(table), 24)]


@pytest.fixture()
def vector_reader():
    r = hpredis.Reader(highway_mode=True)
    r.feed(_bulk(PAYLOAD))
    table, arena = r.gets()
    assert len(_slices(table)) == 1
    return r, table, arena


def test_highway_table_and_arena(vector_reader):
    r, table, arena = vector_reader
    off, length, resp_type = _slices(table)[0]
    assert chr(resp_type) == "$"
    assert length == len(PAYLOAD)
    assert bytes(arena[off : off + length]) == PAYLOAD  # byte-for-byte


def test_numpy_reads_the_arena_without_copying(vector_reader):
    r, table, arena = vector_reader
    off, length, _ = _slices(table)[0]
    arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
    assert arr.shape == (DIM,)
    assert np.shares_memory(arr, memoryview(arena))
    assert arr[0] == 0.0 and arr[DIM - 1] == float(DIM - 1)


def test_numpy_wrap_under_5us(vector_reader):
    r, table, arena = vector_reader
    off, length, _ = _slices(table)[0]
    np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)  # warmup
    times = []
    for _ in range(100):
        t0 = time.perf_counter_ns()
        np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
        times.append(time.perf_counter_ns() - t0)
    assert min(times) < 5000, f"np.frombuffer wrap took {min(times)}ns"


def test_table_and_wrap_allocations_are_size_independent():
    """The arena is the only size-dependent allocation (it is the buffer)."""

    def measure(bulk):
        r = hpredis.Reader(highway_mode=True)
        r.feed(bulk)  # arena allocation happens here, outside the measurement
        tracemalloc.start()
        table, arena = r.gets()
        off, length, _ = _slices(table)[0]
        np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return peak

    small_bulk = _bulk(bytes(len(PAYLOAD)))
    big_bulk = _bulk(bytes(len(PAYLOAD) * 100))
    measure(small_bulk)  # warmup (numpy dtype caching etc.)
    small = measure(small_bulk)  # 6 KB
    big = measure(big_bulk)  # 600 KB
    assert small == big, f"allocs grew with payload: {small} -> {big} bytes"


def test_views_own_the_arena():
    """A numpy array keeps the bytes: the reader may even be collected."""
    r = hpredis.Reader(highway_mode=True)
    r.feed(_bulk(PAYLOAD))
    table, arena = r.gets()
    off, length, _ = _slices(table)[0]
    arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
    del r
    assert arr[0] == 0.0 and arr[DIM - 1] == float(DIM - 1)


def test_held_views_survive_later_feeds():
    """Feeds must never reuse memory a view still points at."""
    r = hpredis.Reader(highway_mode=True)
    r.feed(_bulk(PAYLOAD))
    table, arena = r.gets()
    off, length, _ = _slices(table)[0]
    arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
    for i in range(3):
        r.feed(_bulk(bytes([65 + i]) * 32))
        _table2, _arena2 = r.gets()
    assert arr[0] == 0.0 and arr[DIM - 1] == float(DIM - 1)


def test_arena_stays_bounded_without_views():
    r = hpredis.Reader(highway_mode=True)
    for _ in range(200):
        r.feed(_bulk(PAYLOAD))
        table, arena = r.gets()
    assert len(arena) < 4 * len(PAYLOAD), f"arena grew to {len(arena)}"


def test_real_statuses():
    # a complete reply followed by a partial one: the complete one is returned
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"*2\r\n$3\r\nfoo\r\n:42\r\n$3\r\npa")
    table, arena = r.gets()
    # array header, payload "foo" at 8, text "42" at 14
    assert [(o, l, chr(t)) for o, l, t in _slices(table)] == [
        (0, 2, "*"),
        (8, 3, "$"),
        (14, 2, ":"),
    ]
    assert r.gets() is False  # the partial bulk is not a reply yet
    r.feed(b"r\r\n")
    table, arena = r.gets()
    assert bytes(arena[_slices(table)[0][0] : _slices(table)[0][0] + 3]) == b"par"

    # container rows are typed headers, not "no data yet"
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n")
    table, arena = r.gets()
    assert [(o, l, chr(t)) for o, l, t in _slices(table)] == [
        (0, 2, "%"),
        (8, 1, "$"),
        (15, 1, "$"),
    ]

    # unknown type byte is a protocol error, not an incomplete reply
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"?bad\r\n")
    with pytest.raises(hpredis.ProtocolError):
        r.gets()


@pytest.mark.parametrize(
    "payload",
    [
        b":abc\r\n",
        b":1a\r\n",
        b":01\r\n",
        b",abc\r\n",
        b",+1\r\n",
        b",1e9999\r\n",
        b",1e-9999\r\n",
        b",0x10\r\n",
        b",1_0\r\n",
    ],
)
def test_highway_rejects_malformed_int_and_double(payload):
    # the highway scan used to skip the int/double validation the build path
    # applies, so it accepted replies that classic mode and hiredis reject
    r = hpredis.Reader(highway_mode=True)
    r.feed(payload)
    with pytest.raises(hpredis.ProtocolError):
        r.gets()


@pytest.mark.parametrize(
    "payload",
    [
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
        b"%2\r\n$1\r\nk\r\n:1\r\n$1\r\nv\r\n$1\r\nw\r\n",
        b"*2\r\n*2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n",
        b"*4\r\n$4\r\ndata\r\n$-1\r\n#t\r\n,1.5\r\n",
        b"*2\r\n$3\r\nfoo\r\n*2\r\n$1\r\nx\r\n=8\r\ntxt:abcd\r\n",
    ],
)
def test_highway_chunked_matches_single_feed(payload):
    one = hpredis.Reader(highway_mode=True)
    one.feed(payload)
    want, want_arena = one.gets()
    for chunk in (1, 3, 7, 4096):
        r = hpredis.Reader(highway_mode=True)
        got = False
        for off in range(0, len(payload), chunk):
            r.feed(payload[off : off + chunk])
            got = r.gets()
            if got is not False:
                break
        assert got is not False, chunk
        table, arena = got
        assert bytes(table) == bytes(want), chunk


def test_highway_chunked_protocol_error_matches_single_feed():
    payload = b"*2\r\n$1\r\na\r\n?bad\r\n"
    r = hpredis.Reader(highway_mode=True)
    with pytest.raises(hpredis.ProtocolError):
        for off in range(0, len(payload), 3):
            r.feed(payload[off : off + 3])
            if r.gets() is not False:
                break


def test_highway_verbatim_slice_nil_and_format_error():
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"=8\r\ntxt:abcd\r\n")
    table, arena = r.gets()
    off, length, typ = _slices(table)[0]
    assert (length, chr(typ)) == (4, "=")
    assert bytes(arena[off : off + length]) == b"abcd"

    r = hpredis.Reader(highway_mode=True)
    r.feed(b"=-1\r\n")
    table, arena = r.gets()
    assert _slices(table) == [(0, -1, 61)]

    r = hpredis.Reader(highway_mode=True)
    r.feed(b"=3\r\nabc\r\n")
    with pytest.raises(hpredis.ProtocolError, match="content type"):
        r.gets()


def test_highway_nil_and_container_rows_align():
    # a missing MGET element is a -1 row, so later rows cannot shift up
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"*3\r\n$1\r\na\r\n$-1\r\n$1\r\nc\r\n")
    table, arena = r.gets()
    assert [(o, l, chr(t)) for o, l, t in _slices(table)] == [
        (0, 3, "*"),
        (8, 1, "$"),
        (11, -1, "$"),
        (20, 1, "$"),
    ]


def test_highway_bool_null_and_double_rows():
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"*3\r\n#t\r\n_\r\n,1.5\r\n")
    table, arena = r.gets()
    assert [(o, l, chr(t)) for o, l, t in _slices(table)] == [
        (0, 3, "*"),
        (5, 1, "#"),
        (8, -1, "_"),
        (12, 3, ","),
    ]


def test_error_reply_is_a_typed_slice():
    r = hpredis.Reader(highway_mode=True)
    r.feed(b"-ERR boom\r\n")
    table, arena = r.gets()
    off, length, resp_type = _slices(table)[0]
    assert chr(resp_type) == "-"
    assert bytes(arena[off : off + length]) == b"ERR boom"
