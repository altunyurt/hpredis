"""Robustness regressions for hpredis.

These guard the findings from the second review: reference leaks, a stack
overflow reachable from a partially-fed container stream, unbounded
allocations from hostile length headers, and hiredis' error-text semantics.
"""

import gc
import sys
import tracemalloc

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
