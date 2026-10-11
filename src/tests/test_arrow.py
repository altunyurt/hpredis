"""hpredis.arrow: zero-copy BinaryView import of a highway table."""

import ctypes
import gc

import numpy as np
import pytest

import hpredis

pa = pytest.importorskip("pyarrow")

from hpredis.arrow import binary_array  # noqa: E402


def hw(stream):
    r = hpredis.Reader(highway_mode=True)
    r.feed(stream)
    return r.gets()


def bulks(rows):
    return b"*%d\r\n" % len(rows) + b"".join(
        b"$%d\r\n%s\r\n" % (len(x), x) for x in rows)


def test_binary_view_inline_and_long_payloads():
    rows = [b"", b"abc", b"x" * 12, b"y" * 13, bytes(range(256)), b"last"]
    t, a = hw(bulks(rows))
    arr = binary_array(t, a)
    assert arr.type == pa.binary_view()
    assert arr.null_count == 0
    assert arr.to_pylist() == rows


def test_binary_view_is_zero_copy_and_keeps_the_arena_alive():
    t, a = hw(bulks([b"ab", b"c" * 20]))
    arr = binary_array(t, a)
    assert arr.buffers()[2].address == ctypes.addressof(ctypes.c_char.from_buffer(a))
    # a long payload (> 12 bytes) is not inlined: the array reads the arena,
    # so mutating it is visible (short values are inlined by the Arrow layout)
    a[int(np.frombuffer(t, "<i8").reshape(-1, 3)[2][0])] = ord("Z")
    assert arr[1].as_py() == b"Z" + b"c" * 19
    del t, a
    gc.collect()
    assert arr[1].as_py() == b"Z" + b"c" * 19


def test_nil_rows_become_arrow_nulls():
    t, a = hw(b"*3\r\n$2\r\nab\r\n$-1\r\n$2\r\ncd\r\n")
    arr = binary_array(t, a)
    assert arr.to_pylist() == [b"ab", None, b"cd"]
    assert arr.null_count == 1


def test_bare_scalar_and_empty_array():
    t, a = hw(b"$5\r\nhello\r\n")
    assert binary_array(t, a).to_pylist() == [b"hello"]
    t, a = hw(b"*0\r\n")
    assert len(binary_array(t, a)) == 0


def test_container_rows_are_rejected():
    t, a = hw(b"*2\r\n*1\r\n$1\r\nx\r\n$1\r\ny\r\n")
    with pytest.raises(ValueError, match="is a container"):
        binary_array(t, a)


def test_mget_float32_roundtrip():
    m = np.random.default_rng(0).random((50, 64)).astype(np.float32)
    t, a = hw(bulks([x.tobytes() for x in m]))
    arr = binary_array(t, a)
    got = np.stack([np.frombuffer(v, np.float32) for v in arr.to_pylist()])
    assert np.array_equal(got, m)
