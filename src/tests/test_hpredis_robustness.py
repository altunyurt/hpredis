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


def test_feed_accepts_buffer_protocol_objects():
    import array

    r = hpredis.Reader()
    r.feed(array.array("B", b"+OK\r\n"))
    assert r.gets(False) == b"OK"
    with pytest.raises(TypeError):
        r.feed(42)


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


def test_error_replies_parse_in_order():
    r = hpredis.Reader()
    r.feed(b"-ERR one\r\n-ERR two\r\n")
    first = r.gets()
    assert isinstance(first, hpredis.ReplyError) and first.args[0] == "ERR one"
    # one reply per gets(): the second stays buffered in the arena
    assert r._core.buffered() == len(b"-ERR two\r\n")
    second = r.gets()
    assert isinstance(second, hpredis.ReplyError) and second.args[0] == "ERR two"
    assert r._core.buffered() == 0


def test_reader_release_frees_cached_references():
    def parse_error(msg):
        return hpredis.ReplyError(msg)

    baseline = sys.getrefcount(parse_error)
    r = hpredis.Reader(replyError=parse_error)
    r.feed(b"+OK\r\n")
    assert r.gets() == b"OK"
    for _ in range(100):  # exercises the cached incomplete tuple
        status, payload = r._core.try_gets(0)
        assert status == 1 and payload is None
    del r
    gc.collect()
    assert sys.getrefcount(parse_error) == baseline


def test_del_tolerates_a_core_without_dispose():
    # a Reader built against an older hpredis_core.so has no dispose(); the
    # finalizer must fall back to free() instead of raising into pytest
    class OldCore:
        def __init__(self):
            self.freed = False

        def free(self):
            self.freed = True

    r = hpredis.Reader()
    old = OldCore()
    r._core = old
    r.__del__()
    assert old.freed is True


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


def test_chunked_gets_resumes_large_array_scan():
    count = 20_000
    leaf = b"$64\r\n" + b"x" * 64 + b"\r\n"
    payload = b"*%d\r\n" % count + leaf * count
    r = reader()
    result = False
    for off in range(0, len(payload), 4096):
        r.feed(payload[off : off + 4096])
        value = r.gets(False)
        if value is not False:
            result = value
    assert len(result) == count
    assert result[0] == b"x" * 64
    assert result[-1] == b"x" * 64
    assert r.gets() is False


def test_chunked_large_array_matches_single_feed():
    payload = b"*2000\r\n" + (b"$8\r\n" + b"x" * 8 + b"\r\n") * 2000
    one = hpredis.Reader()
    one.feed(payload)
    want = one.drain(False)
    for chunk in (777, 8192):
        assert repr(_drain_chunked(payload, chunk)) == repr(want)


def test_chunked_large_map_and_set_match_single_feed():
    pairs = b"".join(b"$4\r\nk%03d\r\n:1\r\n" % i for i in range(500))
    items = b"".join(b"$4\r\nv%03d\r\n" % i for i in range(500))
    for payload in (b"%500\r\n" + pairs, b"~500\r\n" + items):
        one = hpredis.Reader()
        one.feed(payload)
        want = one.drain(False)
        for chunk in (1, 7, 777, 8192):
            assert repr(_drain_chunked(payload, chunk)) == repr(want)


def _chunked_outcome(payload, chunk):
    """Replies (or the first exception) from feeding in chunks and draining."""
    r = hpredis.Reader()
    out = []
    try:
        for off in range(0, len(payload), chunk):
            r.feed(payload[off : off + chunk])
            out.extend(r.drain(False))
        return ("ok", out)
    except Exception as exc:  # noqa: BLE001
        return (type(exc).__name__, str(exc))


def _norm(v):
    if type(v).__name__ == "PushNotification":
        return ("PUSH", _norm(list(v)))
    if isinstance(v, list):
        return [_norm(x) for x in v]
    if isinstance(v, dict):
        return sorted((_norm(k), _norm(x)) for k, x in v.items())
    if isinstance(v, set):
        return sorted(_norm(x) for x in v)
    return v


def test_chunked_deep_nesting_matches_single_feed():
    payload = (
        b"*2\r\n"
        b"*3\r\n"
        b"%2\r\n$1\r\nk\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nv\r\n:1\r\n"
        b"~2\r\n$1\r\nx\r\n$1\r\nr\r\n"
        b"+done\r\n"
        b">1\r\n$1\r\np\r\n"
    )
    one = hpredis.Reader()
    one.feed(payload)
    want = [_norm(x) for x in one.drain(False)]
    for chunk in (1, 3, 7, 777):
        outcome = _chunked_outcome(payload, chunk)
        assert outcome[0] == "ok", (chunk, outcome)
        assert [_norm(x) for x in outcome[1]] == want, chunk


def test_chunked_max_depth_matches_single_feed():
    for payload in (b"*1\r\n" * 1024 + b":1\r\n", b"*1\r\n" * 1025 + b":1\r\n"):
        one = hpredis.Reader()
        one.feed(payload)
        try:
            want = ("ok", one.drain(False))
        except Exception as exc:  # noqa: BLE001
            want = (type(exc).__name__, str(exc))
        for chunk in (1, 7, 4096):
            outcome = _chunked_outcome(payload, chunk)
            if want[0] == "ok":
                assert outcome[0] == "ok", (chunk, outcome)
                depth, leaf = 0, outcome[1][0]
                while isinstance(leaf, list) and len(leaf) == 1:
                    leaf, depth = leaf[0], depth + 1
                assert (depth, leaf) == (1024, 1), chunk
            else:
                assert outcome == want, (chunk, outcome, want)


def test_chunked_nested_protocol_error_matches_single_feed():
    payload = b"*2\r\n*2\r\n$1\r\na\r\n?bad\r\n+tail\r\n"
    one = hpredis.Reader()
    one.feed(payload)
    try:
        want = ("ok", one.drain(False))
    except Exception as exc:  # noqa: BLE001
        want = (type(exc).__name__, str(exc))
    for chunk in (1, 5, 4096):
        assert _chunked_outcome(payload, chunk) == want, chunk


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


def test_gets_tracks_unread_bytes():
    r = reader()
    r.feed(b"+A\r\n$1\r\nb\r\n:7\r\n")
    assert r.gets() == b"A"
    assert r.len() == len(b"$1\r\nb\r\n:7\r\n")
    assert r.has_data()
    assert r.gets() == b"b"
    assert r.len() == len(b":7\r\n")
    assert r.gets() == 7
    assert r.len() == 0
    assert not r.has_data()
    assert r.gets() is False
    assert r.len() == 0


# --- gets(): direct success return across the Mojo/Python boundary ----------


def test_gets_consumes_one_reply_per_call():
    data = b"+OK\r\n" * 5000
    r = reader()
    r.feed(data)
    assert r.gets() == b"OK"
    # no read-ahead queue: everything else stays in the core arena
    assert r.has_data()
    assert r.len() == len(data) - len(b"+OK\r\n")
    assert r.drain() == [b"OK"] * 4999
    assert r.len() == 0
    assert not r.has_data()


def test_gets_keeps_partial_tail_for_next_feed():
    r = reader()
    r.feed(b"+one\r\n+par")
    assert r.gets() == b"one"
    assert r.len() == len(b"+par")
    assert r.gets() is False
    r.feed(b"tial\r\n+two\r\n")
    assert r.gets() == b"partial"
    assert r.gets() == b"two"
    assert r.gets() is False
    assert r.len() == 0


def test_reply_error_factory_runs_once_per_error_reply():
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


def test_replies_after_set_encoding_decode_in_drain():
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


@pytest.mark.parametrize(
    "payload",
    [
        b"$9223372036854775807\r\n",
        b"=9223372036854775807\r\n",
    ],
)
def test_huge_length_header_does_not_crash(payload):
    # before the overflow-safe check `pstart + blen + 2 > end` wrapped
    # negative for Int64-sized lengths and the parse segfaulted
    r = reader()
    r.feed(payload)
    assert r.gets(False) is False


@pytest.mark.parametrize(
    "payload",
    [
        b"$9223372036854775808\r\n",   # Int64.max + 1
        b"$18446744073709551614\r\n",  # UInt64.max - 1
        b"$18446744073709551615\r\n",  # UInt64.max: used to wrap to nil
        b"$18446744073709551616\r\n",  # UInt64 overflow
        b"=18446744073709551615\r\n",  # the same cap on verbatim lengths
    ],
)
def test_over_int64_lengths_match_hiredis(payload):
    # _read_len_fast used to return Int(value) for any UInt64, so UInt64.max
    # wrapped to -1 and $18446744073709551615 came back as a nil reply;
    # hiredis rejects every length above Int64.max as "Bad bulk string length"
    outcomes = []
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.feed(payload)
        try:
            outcomes.append(("ok", repr(r.gets(False))))
        except Exception as exc:
            outcomes.append((type(exc).__name__, str(exc)))
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize(
    "payload",
    [
        b"=-1\r\n",
        b"=-2\r\n",
        b"=-9223372036854775808\r\n",
        b"=0\r\n\r\n",
        b"=3\r\nabc\r\n",
        b"=4\r\nabcd\r\n",
        b"=4\r\na:bc\r\n",
        b"=5\r\nabc:d\r\n",
        b"=5\r\nab:cd\r\n",
        b"=6\r\nTXT:xy\r\n",
        b"=8\r\ntxt:abcd\r\n",
    ],
)
def test_verbatim_length_and_format_match_hiredis(payload):
    # a verbatim payload needs a 4-byte "xxx:" specifier; -1 is nil, below -1
    # is a bulk range error. The old code searched for any colon and read past
    # the reply when there was none.
    outcomes = []
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.feed(payload)
        try:
            outcomes.append(("ok", repr(r.gets(False))))
        except Exception as exc:
            outcomes.append((type(exc).__name__, str(exc)))
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize(
    "payload",
    [
        b",abc\r\n",
        b",\r\n",
        b",1 \r\n",
        b",+1\r\n",
        b",infinity\r\n",
        b",1e9999\r\n",
        b",1e-9999\r\n",
        b",0x10\r\n",
        b",1_0\r\n",
        b",--1\r\n",
        b"," + b"1" * 400 + b"\r\n",
        b"#x\r\n",
        b"#\r\n",
        b"#true\r\n",
        b"#t \r\n",
    ],
)
def test_double_and_bool_errors_match_hiredis(payload):
    outcomes = []
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.feed(payload)
        try:
            outcomes.append(("ok", repr(r.gets(False))))
        except Exception as exc:
            outcomes.append((type(exc).__name__, str(exc)))
    assert outcomes[0] == outcomes[1]


def test_valid_doubles_and_bools_still_parse():
    import math

    for payload, expected in (
        (b",1.5\r\n", 1.5),
        (b",inf\r\n", float("inf")),
        (b",-1.5e308\r\n", -1.5e308),
        (b"#T\r\n", True),
        (b"#f\r\n", False),
    ):
        r = hpredis.Reader()
        r.feed(payload)
        assert r.gets(False) == expected
    r = hpredis.Reader()
    r.feed(b",nan\r\n")
    assert math.isnan(r.gets(False))


@pytest.mark.parametrize(
    "payload",
    [
        b"*4294967296\r\n",
        b">4294967296\r\n",
        b"%4294967296\r\n",
        b"~4294967296\r\n",
        b"|4294967296\r\n",
    ],
)
def test_container_count_range_matches_hiredis(payload):
    # above UINT32_MAX hiredis raises ProtocolError instead of buffering
    outcomes = []
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.feed(payload)
        try:
            outcomes.append(("ok", repr(r.gets(False))))
        except Exception as exc:
            outcomes.append((type(exc).__name__, str(exc)))
    assert outcomes[0] == outcomes[1]


def test_resp3_bignum_and_attribute_match_hiredis():
    for payload in (
        b"(3492890328409238509324850943850943825024385\r\n",
        b"(9223372036854775807\r\n",
        b"(123\r\n",
        b"|1\r\n+key-popularity\r\n,1.0\r\n",
    ):
        results = []
        for mod in (hiredis, hpredis):
            r = mod.Reader()
            r.feed(payload)
            results.append(r.gets(False))
        assert repr(results[0]) == repr(results[1])


def test_nested_unhashable_map_key_raises_type_error():
    r = reader()
    r.feed(b"*1\r\n%1\r\n*1\r\n:1\r\n$1\r\nv\r\n")
    # CPython's own message, exactly as hiredis surfaces it
    with pytest.raises(TypeError, match="unhashable type: 'list'"):
        r.gets(False)


@pytest.mark.parametrize(
    "payload",
    [
        b"%1\r\n*1\r\n:1\r\n$1\r\nv\r\n!5\r\nboom\r\n",
        b"*1\r\n%1\r\n*1\r\n:1\r\n$1\r\nv\r\n",
        b"!5\r\nboom\r\n%1\r\n*1\r\n:1\r\n$1\r\nv\r\n",
    ],
)
def test_unhashable_key_first_error_matches_hiredis(payload):
    # only the first outcome is compared: after its own dict-insert failure
    # hiredis' reader reports "Out of memory" for everything that follows
    outcomes = []
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.feed(payload)
        try:
            outcomes.append(("ok", repr(r.gets(False))))
        except Exception as exc:
            outcomes.append((type(exc).__name__, str(exc)))
    assert outcomes[0] == outcomes[1]


def test_map_value_error_marker_is_finalized():
    r = reader()
    r.feed(b"%1\r\n+key\r\n-ERR inner\r\n")
    out = r.gets()
    assert isinstance(out[b"key"], hpredis.ReplyError)
    assert out[b"key"].args[0] == "ERR inner"


def test_push_with_nested_error_is_a_notification_of_errors():
    r = reader()
    r.feed(b">1\r\n-ERR pushed\r\n")
    out = r.gets()
    assert isinstance(out, hpredis.PushNotification)
    assert isinstance(out[0], hpredis.ReplyError)


def _reply_shape(obj):
    if isinstance(obj, list):
        return (type(obj).__name__, tuple(_reply_shape(x) for x in obj))
    if isinstance(obj, dict):
        return ("dict", tuple(sorted((_reply_shape(k), _reply_shape(v)) for k, v in obj.items())))
    return type(obj).__name__


@pytest.mark.parametrize(
    "payload",
    [
        b">1\r\n+hello\r\n",
        b"*1\r\n>1\r\n+hello\r\n",
        b"*2\r\n>1\r\n+a\r\n>1\r\n+b\r\n",
        b"%1\r\n+k\r\n>1\r\n+v\r\n",
        b"~1\r\n>1\r\n+x\r\n",
    ],
)
def test_push_shapes_match_hiredis(payload):
    shapes = []
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.feed(payload)
        shapes.append(_reply_shape(r.gets(False)))
    assert shapes[0] == shapes[1]


def test_drain_wraps_top_level_pushes():
    r = hpredis.Reader()
    r.feed(b">1\r\n+hello\r\n")
    out = r.drain()
    assert len(out) == 1
    assert isinstance(out[0], hpredis.PushNotification)


def test_set_encoding_and_drain_do_not_leak_references():
    enc = "".join(["ut", "f-", "8"])
    r = hpredis.Reader()
    base = sys.getrefcount(enc)
    for _ in range(50):
        r.set_encoding(enc)
    # the active encoding is held exactly once by the wrapper
    assert sys.getrefcount(enc) == base + 1

    def parse_error(msg):
        return hpredis.ReplyError(msg)

    r = hpredis.Reader(replyError=parse_error)
    base = sys.getrefcount(parse_error)
    r.feed(b"+OK\r\n")
    for _ in range(50):
        r.drain()
    assert sys.getrefcount(parse_error) == base


def test_highway_empty_buffer_returns_not_enough_data():
    r = hpredis.Reader(highway_mode=True)
    assert r.gets() is False


def test_maxbuf_defaults_and_reset_match_hiredis():
    assert hiredis.Reader().getmaxbuf() == hpredis.Reader().getmaxbuf() == 16384
    r = hpredis.Reader()
    r.setmaxbuf(100)
    assert r.getmaxbuf() == 100
    r.setmaxbuf(None)
    assert r.getmaxbuf() == 16384


def test_maxbuf_is_not_an_input_limit():
    # hiredis stores maxbuf for idle trimming; it never rejects large replies
    for mod in (hiredis, hpredis):
        r = mod.Reader()
        r.setmaxbuf(1024)
        r.feed(b"$4096\r\n" + b"x" * 4096 + b"\r\n")
        assert r.gets(False) == b"x" * 4096
        r = mod.Reader()
        r.setmaxbuf(1024)
        r.feed(b"$1048576\r\n" + b"x" * 10)
        assert r.gets(False) is False
        assert r.len() == 20


def test_exports_and_error_hierarchy_match_hiredis():
    assert issubclass(hpredis.ProtocolError, hpredis.HiredisError)
    assert issubclass(hpredis.ReplyError, hpredis.HiredisError)
    assert not hasattr(hpredis.Reader, "__len__")  # hiredis has no __len__
    major, minor = (int(p) for p in hpredis.__version__.split(".")[:2])
    assert major > 3 or (major == 3 and minor >= 2)  # redis-py's HIREDIS gate
    assert "Reader" in hpredis.__all__


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
        raise failure

    r = hpredis.Reader(replyError=parse_error)
    r.feed(b"+OK\r\n-ERR one\r\n+NEXT\r\n")
    # replies parsed before the failing one are delivered, not discarded
    assert r.drain() == [b"OK"]
    assert calls == ["ERR one"]
    with pytest.raises(Boom) as excinfo:
        r.drain()
    assert excinfo.value is failure
    assert calls == ["ERR one", "ERR one"]
    # the raising drain consumed the error reply; the rest is pending
    assert r.gets() == b"NEXT"


def test_drain_max_replies_caps_the_batch():
    r = hpredis.Reader()
    r.feed(b"+a\r\n" * 10)
    assert r.drain(max_replies=3) == [b"a"] * 3
    assert r.drain(max_replies=3) == [b"a"] * 3
    assert r.drain(max_replies=100) == [b"a"] * 4
    assert r.drain() == []
    with pytest.raises(ValueError):
        r.drain(max_replies=-1)


def test_drain_delivers_replies_before_a_protocol_error():
    r = hpredis.Reader()
    r.feed(b":1\r\n:2\r\n?bad\r\n")
    assert r.drain(False) == [1, 2]
    with pytest.raises(hpredis.ProtocolError):
        r.drain(False)
    with pytest.raises(hpredis.ProtocolError):  # sticky, like gets()
        r.gets(False)


def test_drain_delivers_replies_before_unhashable_map_key():
    r = hpredis.Reader()
    r.feed(b":1\r\n%1\r\n*1\r\n:1\r\n$1\r\nv\r\n")
    assert r.drain(False) == [1]
    with pytest.raises(TypeError):
        r.drain(False)


def test_drain_delivers_replies_before_decode_failure():
    r = hpredis.Reader(encoding="utf-8")
    r.feed(b"+ok\r\n$2\r\n\xff\xfe\r\n")
    assert r.drain() == ["ok"]
    with pytest.raises(UnicodeDecodeError):
        r.drain()


def test_drain_still_converts_nested_markers():
    r = hpredis.Reader()
    r.feed(b"*2\r\n$1\r\na\r\n-ERR nested\r\n")
    replies = r.drain()
    assert isinstance(replies[0][1], hpredis.ReplyError)
    assert replies[0][1].args[0] == "ERR nested"


# --- cached refs and factory lifetime ---------------------------------------


def test_array_nil_elements_are_none():
    r = reader()
    r.feed(b"*3\r\n$-1\r\n_\r\n$1\r\nx\r\n")
    assert r.gets() == [None, None, b"x"]


def test_close_then_feed_keeps_the_reply_error_factory():
    class Boom(Exception):
        pass

    calls = []

    def factory(message):
        calls.append(message)
        return Boom(message)

    r = hpredis.Reader(replyError=factory)
    r.feed(b"-ERR one\r\n")
    assert isinstance(r.gets(), Boom)
    r.close()
    r.feed(b"-ERR two\r\n")
    assert isinstance(r.gets(), Boom)
    assert calls == ["ERR one", "ERR two"]


def test_drain_error_replies_call_the_factory_once_each():
    class Boom(Exception):
        pass

    calls = []

    def factory(message):
        calls.append(message)
        return Boom(message)

    r = hpredis.Reader(replyError=factory)
    r.feed(b"-ERR one\r\n+OK\r\n-ERR two\r\n")
    replies = r.drain(False)
    assert [type(x) for x in replies] == [Boom, bytes, Boom]
    assert calls == ["ERR one", "ERR two"]


def test_drain_raising_factory_delivers_prior_replies_then_raises():
    class Boom(Exception):
        pass

    def factory(message):
        raise Boom(message)

    r = hpredis.Reader(replyError=factory)
    r.feed(b"+OK\r\n-ERR x\r\n")
    assert r.drain(False) == [b"OK"]
    with pytest.raises(Boom, match="ERR x"):
        r.drain(False)
