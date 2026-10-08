"""Robustness regressions for hpredis.

These guard the findings from the second review: reference leaks, a stack
overflow reachable from a partially-fed container stream, unbounded
allocations from hostile length headers, and hiredis' error-text semantics.
"""

import gc
import sys
import tracemalloc

import hiredis
import pytest

import hpredis


def reader(**kwargs):
    return hpredis.Reader(**kwargs)


# --- reference leaks -------------------------------------------------------


def test_feed_does_not_leak_references():
    data = b"x" * 100
    r = reader()
    base = sys.getrefcount(data)
    for _ in range(1000):
        r.feed(data)
    assert sys.getrefcount(data) == base


def _tracemalloc_growth(fn, iterations):
    gc.collect()
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    fn(iterations)
    gc.collect()
    after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    return sum(s.size_diff for s in after.compare_to(before, "filename"))


def test_error_replies_do_not_leak():
    r = reader()

    def run(n):
        for _ in range(n):
            r.feed(b"-ERR boom\r\n")
            r.gets(False)

    # ~40 bytes leaked per reply before the fix (~800 KiB here)
    assert _tracemalloc_growth(run, 20000) < 64 * 1024


def test_resp3_maps_do_not_leak():
    r = reader()

    def run(n):
        for _ in range(n):
            r.feed(b"%1\r\n$4\r\nkey1\r\n$4\r\nval1\r\n")
            r.gets(False)

    # key and value were leaked per map before the fix (~1.4 MiB here)
    assert _tracemalloc_growth(run, 20000) < 64 * 1024


# --- hostile input ---------------------------------------------------------


def test_partial_container_stream_raises_instead_of_crashing():
    # every fragment nests inside the previous incomplete array; before the
    # depth cap this recursed until the Mojo stack overflowed (segfault)
    r = reader()
    with pytest.raises(hpredis.ProtocolError, match="Max nesting depth exceeded"):
        for _ in range(2000):
            r.feed(b"*3\r\n$1\r\na\r\n")
            r.gets(False)


def test_nesting_depth_cap_matches_hiredis():
    # 1024 nested containers parse (hiredis 3.4.2 allows exactly that many)
    r = reader()
    r.feed(b"*1\r\n" * 1024 + b":1\r\n")
    assert r.gets(False) is not False
    # the 1025th is a sticky protocol error
    r = reader()
    r.feed(b"*1\r\n" * 1025 + b":1\r\n")
    with pytest.raises(hpredis.ProtocolError, match="Max nesting depth exceeded"):
        r.gets(False)
    with pytest.raises(hpredis.ProtocolError, match="Max nesting depth exceeded"):
        r.gets(False)


@pytest.mark.parametrize("payload", [b"*100000000\r\n", b"%99999999\r\n", b"~100000000\r\n"])
def test_huge_length_headers_wait_for_data(payload):
    # notEnoughData, not an eager PyList_New(huge) allocation
    r = reader()
    r.feed(payload)
    assert r.gets(False) is False
    # and a real element count that the buffer can hold still parses
    n = 1000
    r = reader()
    r.feed(b"*%d\r\n" % n + b"$1\r\na\r\n" * n)
    assert len(r.gets(False)) == n


# --- error text semantics --------------------------------------------------


@pytest.mark.parametrize("should_decode", [True, False])
def test_error_text_is_always_str(should_decode):
    # hiredis passes error text through the "s" format: str whatever
    # should_decode says, and regardless of any configured encoding
    r = reader()
    r.feed(b"-ERR boom\r\n")
    assert isinstance(r.gets(should_decode).args[0], str)

    r = reader()
    r.feed(b"?bad\r\n")
    with pytest.raises(hpredis.ProtocolError) as excinfo:
        r.gets(should_decode)
    assert isinstance(excinfo.value.args[0], str)


def test_nested_error_markers_are_text_without_decoding():
    r = reader()
    r.feed(b"*1\r\n-ERR nested\r\n")
    out = r.gets(False)
    assert isinstance(out[0].args[0], str)


def test_bulk_strings_still_honour_should_decode():
    # unlike errors, payload strings are bytes unless decoding is on
    r = reader(encoding="utf-8")
    r.feed(b"$3\r\nabc\r\n")
    assert r.gets(False) == b"abc"
    r = reader(encoding="utf-8")
    r.feed(b"$3\r\nabc\r\n")
    assert r.gets(True) == "abc"


# --- native decoding (decoded in the core at leaf creation) ----------------


def test_drain_decodes_and_honours_should_decode():
    r = hpredis.Reader(encoding="utf-8")
    r.feed(b"+a\r\n$1\r\nb\r\n")
    assert r.drain() == ["a", "b"]
    r = hpredis.Reader(encoding="utf-8")
    r.feed(b"+a\r\n$1\r\nb\r\n")
    assert r.drain(False) == [b"a", b"b"]


def test_set_encoding_none_disables_core_decoding():
    snowman = b"\xe2\x98\x83"
    r = hpredis.Reader(encoding="utf-8")
    r.feed(b"$3\r\n" + snowman + b"\r\n" + b"$3\r\n" + snowman + b"\r\n")
    assert r.gets() == snowman.decode()
    r.set_encoding(encoding=None, errors=None)
    assert r.gets() == snowman


def test_strict_decode_failure_matches_hiredis():
    data = b"*2\r\n$1\r\na\r\n$2\r\n\xff\xfe\r\n"
    messages = []
    for mod in (hiredis, hpredis):
        r = mod.Reader(encoding="utf-8")
        r.feed(data)
        with pytest.raises(UnicodeDecodeError) as excinfo:
            r.gets(True)
        messages.append(str(excinfo.value))
    assert messages[0] == messages[1]


def test_decode_leaves_are_str_in_containers_and_maps():
    r = hpredis.Reader(encoding="utf-8")
    r.feed(b"%1\r\n$1\r\nk\r\n*2\r\n$1\r\na\r\n:1\r\n")
    assert r.gets() == {"k": ["a", 1]}


# --- chunked feeding (scan-then-build path) --------------------------------


def _drain_chunked(payload, chunk, use_gets=False):
    r = hpredis.Reader()
    out = []
    for off in range(0, len(payload), chunk):
        r.feed(payload[off : off + chunk])
        if use_gets:
            while True:
                value = r.gets(False)
                if value is False:
                    break
                out.append(value)
        else:
            out.extend(r.drain(False))
    return out


@pytest.mark.parametrize("chunk", [1, 2, 3, 7, 64, 4096, 65536])
@pytest.mark.parametrize("use_gets", [False, True])
def test_chunked_feeding_matches_single_feed(chunk, use_gets):
    payload = (
        b"*7\r\n$3\r\nfoo\r\n:42\r\n+bar\r\n$0\r\n\r\n$-1\r\n-ERR x\r\n"
        b"*3\r\n$1\r\na\r\n:7\r\n$1\r\nb\r\n"
    ) * 20
    one = hpredis.Reader()
    one.feed(payload)
    want = one.drain(False)
    assert repr(_drain_chunked(payload, chunk, use_gets)) == repr(want)


def test_chunked_large_array_matches_single_feed():
    payload = b"*2000\r\n" + (b"$8\r\n" + b"x" * 8 + b"\r\n") * 2000
    one = hpredis.Reader()
    one.feed(payload)
    want = one.drain(False)
    for chunk in (777, 8192):
        assert repr(_drain_chunked(payload, chunk)) == repr(want)


def test_chunked_protocol_error_is_reported_once_complete():
    # the error sits past the first chunk boundary: both the scan and the
    # builder must agree that the reply is complete before reporting it
    payload = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n?bad\r\n"
    r = hpredis.Reader()
    r.feed(payload[:12])
    assert r.gets(False) is False
    r.feed(payload[12:])
    assert r.gets(False) == [b"a", b"b"]
    with pytest.raises(hpredis.ProtocolError):
        r.gets(False)


# --- arena lifetime --------------------------------------------------------


def test_close_releases_arena_and_is_idempotent():
    r = hpredis.Reader()
    r.feed(b"+OK\r\n")
    assert r.gets(False) == b"OK"
    r.close()
    r.close()  # no double free
    # the reader stays usable: the arena is reallocated on the next feed
    r.feed(b"+AGAIN\r\n")
    assert r.gets(False) == b"AGAIN"


def test_reader_gc_does_not_leak_the_arena():
    # the arena is raw C memory the Mojo struct does not own; the wrapper's
    # __del__ frees it.  Bounded on purpose (500 x 64KB), and peak RSS is
    # checked so a regression fails loudly here instead of OOM-ing a machine.
    import resource

    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    for _ in range(500):
        r = hpredis.Reader()
        r.feed(b"$65536\r\n" + b"x" * 65536 + b"\r\n")
        r.gets(False)
        del r
    gc.collect()
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    # 500 x 64KB is 32MB of arena; leaking it would show up as ~64MB peak
    assert peak - before < 32


# --- gets(): bounded read-ahead --------------------------------------------


def test_gets_prefetches_clean_replies_and_tracks_unread_bytes():
    r = reader()
    r.feed(b"+A\r\n$1\r\nb\r\n:7\r\n")
    assert r.gets() == b"A"
    assert r._core.buffered() == 0
    assert r.len() == len(b"$1\r\nb\r\n:7\r\n")
    assert r.has_data()
    assert r.gets() == b"b"
    assert r.len() == len(b":7\r\n")
    assert r.gets() == 7
    assert r.len() == 0
    assert r.gets() is False


# --- gets(): direct success return across the Mojo/Python boundary ----------


def test_gets_read_ahead_queue_is_bounded():
    data = b"+OK\r\n" * 5000
    r = reader()
    r.feed(data)
    assert r.gets() == b"OK"
    assert r._pending_count - r._pending_index == 4095
    assert r._core.buffered() == (5000 - 4096) * len(b"+OK\r\n")
    assert r.len() == len(data) - len(b"+OK\r\n")
    assert r.drain() == [b"OK"] * 4999
    assert r.len() == 0


def test_gets_prefetch_keeps_partial_tail_for_next_feed():
    r = reader()
    r.feed(b"+one\r\n+par")
    assert r.gets() == b"one"
    assert r.len() == len(b"+par")
    assert r.gets() is False
    r.feed(b"tial\r\n+two\r\n")
    assert r.gets() == b"partial"
    assert r.gets() == b"two"
    assert r.gets() is False
    assert len(r) == 0


def test_gets_prefetch_does_not_run_reply_error_early():
    calls = []

    class Boom(Exception):
        pass

    def parse_error(message):
        calls.append(message)
        raise Boom(message)

    r = reader(replyError=parse_error)
    r.feed(b"+first\r\n-ERR failure\r\n+last\r\n")
    assert r.gets() == b"first"
    assert calls == []
    with pytest.raises(Boom, match="ERR failure"):
        r.gets()
    assert calls == ["ERR failure"]
    assert r.gets() == b"last"


def test_prefetched_replies_honor_later_encoding_and_drain():
    r = reader()
    r.feed(b"$1\r\nx\r\n$1\r\ny\r\n")
    assert r.gets(False) == b"x"
    r.set_encoding("utf-8")
    assert r.drain() == ["y"]
    assert r.len() == 0


def test_core_gets_returns_clean_reply_without_status_tuple():
    r = reader()
    r.feed(b"+OK\r\n")
    assert r._core.try_gets(0) == b"OK"


# --- drain(): error replies built in the core ------------------------------


def test_drain_builds_error_instances_in_the_core():
    r = hpredis.Reader()
    r.feed(b"+OK\r\n-ERR one\r\n+NEXT\r\n")
    replies = r.drain()
    assert [type(x).__name__ for x in replies] == ["bytes", "ReplyError", "bytes"]
    assert replies[1].args[0] == "ERR one"       # str, always (hiredis parity)


def test_drain_reports_a_raising_reply_error_and_stays_consistent():
    class Boom(Exception):
        pass

    calls = []
    failure = Boom("ERR one")

    def parse_error(msg):
        calls.append(msg)
        if len(calls) == 1:
            raise failure
        return "unexpected second invocation"

    r = hpredis.Reader(replyError=parse_error)
    r.feed(b"+OK\r\n-ERR one\r\n+NEXT\r\n")
    with pytest.raises(Boom) as excinfo:
        r.drain()
    assert excinfo.value is failure
    assert calls == ["ERR one"]
    # the error reply was consumed, the rest of the batch is still pending
    assert r.gets() == b"NEXT"


def test_drain_still_converts_nested_markers():
    r = hpredis.Reader()
    r.feed(b"*2\r\n$1\r\na\r\n-ERR nested\r\n")
    replies = r.drain()
    assert isinstance(replies[0][1], hpredis.ReplyError)
    assert replies[0][1].args[0] == "ERR nested"
