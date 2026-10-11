"""Highway table decoders: highway_ints / highway_floats / highway_gather.

These read a (table, arena) pair from Reader(highway_mode=True).gets() in one
native pass.  The tests pin the row contract (payload, nil, container), the
auto first_row rule (skip the reply's own container header, keep a scalar
reply's row) and the error text for bad tables, bad payloads and bad rows.
"""

import random
import struct

import numpy as np
import pytest

import hpredis


def hw(stream):
    r = hpredis.Reader(highway_mode=True)
    r.feed(stream)
    return r.gets()


def bulk(b):
    return b"$%d\r\n%s\r\n" % (len(b), b)


def arr(items):
    return b"*%d\r\n" % len(items) + b"".join(items)


def ints(table, arena, *args):
    return np.frombuffer(hpredis.highway_ints(table, arena, *args), "<i8")


def floats(table, arena, *args):
    return np.frombuffer(hpredis.highway_floats(table, arena, *args), "<f8")


def test_ints_roundtrip_boundaries_and_random():
    rnd = random.Random(1)
    vals = [0, 1, -1, 42, 2**63 - 1, -2**63, 10**12, -10**12] + [
        rnd.randint(-2**63, 2**63 - 1) for _ in range(5000)]
    t, a = hw(arr([bulk(str(v).encode()) for v in vals]))
    assert np.array_equal(ints(t, a), np.array(vals, dtype=np.int64))


def test_ints_leading_zeros_and_first_row():
    t, a = hw(arr([bulk(b"007"), bulk(b"-0")]))
    assert list(ints(t, a)) == [7, 0]
    assert len(hpredis.highway_ints(t, a, 2)) == 8
    assert len(hpredis.highway_ints(t, a, 3)) == 0
    with pytest.raises(ValueError, match="first_row out of range"):
        hpredis.highway_ints(t, a, 4)
    with pytest.raises(ValueError, match="first_row out of range"):
        hpredis.highway_ints(t, a, -2)


@pytest.mark.parametrize(
    "payload",
    [b"", b"-", b"1a", b"+1", b" 1", b"9223372036854775808",
     b"-9223372036854775809", b"1" * 30],
)
def test_ints_reject_malformed(payload):
    t, a = hw(arr([bulk(payload)]))
    with pytest.raises(ValueError, match="row 1"):
        hpredis.highway_ints(t, a)


def test_ints_reject_nil_and_container_rows():
    t, a = hw(arr([b"$-1\r\n"]))
    with pytest.raises(ValueError, match="row 1 is nil or a container"):
        hpredis.highway_ints(t, a)
    t, a = hw(arr([arr([bulk(b"1")])]))
    with pytest.raises(ValueError, match="row 1 is nil or a container"):
        hpredis.highway_ints(t, a)


def test_ints_accept_resp_int_rows():
    t, a = hw(arr([b":5\r\n"]))
    assert bytes(hpredis.highway_ints(t, a)) == struct.pack("<q", 5)


def test_decoders_reject_bad_table_arena_and_arity():
    t, a = hw(arr([bulk(b"1")]))
    with pytest.raises(ValueError, match="malformed table"):
        hpredis.highway_ints(bytearray(5), a)
    with pytest.raises(ValueError, match="lies outside the arena"):
        hpredis.highway_ints(t, bytearray(2))
    with pytest.raises(TypeError):
        hpredis.highway_ints(1, a)
    with pytest.raises(TypeError):
        hpredis.highway_ints(t)


def test_floats_roundtrip_including_infinities():
    rnd = random.Random(2)
    vals = [0.0, 1.5, -2.25, 1e10, 3e-5, 123456.789, float("inf"), -float("inf")]
    vals += [rnd.uniform(-1e6, 1e6) for _ in range(2000)]
    t, a = hw(arr([bulk(repr(v).encode()) for v in vals]))
    assert np.array_equal(floats(t, a), np.array(vals))


@pytest.mark.parametrize(
    "payload", [b"", b"abc", b"+1.0", b"0x10", b"1e999", b"1_0", b"1" * 400]
)
def test_floats_reject_malformed(payload):
    t, a = hw(arr([bulk(payload)]))
    with pytest.raises(ValueError, match="row 1"):
        hpredis.highway_floats(t, a)


def test_gather_roundtrip_and_out_buffer():
    rnd = random.Random(3)
    rows = [bytes(rnd.getrandbits(8) for _ in range(16)) for _ in range(300)]
    t, a = hw(arr([bulk(r) for r in rows]))
    assert bytes(hpredis.highway_gather(t, a)) == b"".join(rows)
    out = np.zeros((300, 16), np.uint8)
    ret = hpredis.highway_gather(t, a, -1, 16, out)
    assert ret is out and out.tobytes() == b"".join(rows)


def test_gather_width_inference_and_mismatch():
    t, a = hw(arr([bulk(b"ab"), bulk(b"cd")]))
    assert bytes(hpredis.highway_gather(t, a)) == b"abcd"
    with pytest.raises(ValueError, match="length differs from width"):
        hpredis.highway_gather(t, a, -1, 8)
    t, a = hw(arr([bulk(b"ab"), bulk(b"abc")]))
    with pytest.raises(ValueError, match="length differs from width"):
        hpredis.highway_gather(t, a)


def test_gather_rejects_nil_row_and_small_out():
    t, a = hw(arr([bulk(b"ab"), b"$-1\r\n"]))
    with pytest.raises(ValueError, match="row 2 is nil or a container"):
        hpredis.highway_gather(t, a)
    t, a = hw(arr([bulk(b"ab"), bulk(b"cd")]))
    with pytest.raises(ValueError, match="smaller than"):
        hpredis.highway_gather(t, a, -1, 2, np.zeros(2, np.uint8))


def test_gather_empty_array_and_mget_float32():
    t, a = hw(arr([]))
    assert bytes(hpredis.highway_gather(t, a)) == b""
    m = np.random.default_rng(4).random((100, 256)).astype(np.float32)
    t, a = hw(arr([bulk(x.tobytes()) for x in m]))
    got = np.frombuffer(hpredis.highway_gather(t, a), np.float32).reshape(100, 256)
    assert np.array_equal(got, m)


def test_auto_first_row_keeps_scalar_replies():
    t, a = hw(b"$5\r\nhello\r\n")
    assert bytes(hpredis.highway_gather(t, a)) == b"hello"
    t, a = hw(b":42\r\n")
    assert list(ints(t, a)) == [42]
    t, a = hw(b"$3\r\n1.5\r\n")
    assert list(floats(t, a)) == [1.5]
    # explicit 0 exposes the reply's own header row, which is not a payload
    t, a = hw(arr([bulk(b"x")]))
    with pytest.raises(ValueError, match="row 0 is nil or a container"):
        hpredis.highway_gather(t, a, 0)
    # a nil scalar has no payload; a nil container has no rows
    t, a = hw(b"$-1\r\n")
    with pytest.raises(ValueError, match="row 0 is nil or a container"):
        hpredis.highway_gather(t, a)
    t, a = hw(b"*-1\r\n")
    assert bytes(hpredis.highway_gather(t, a)) == b""


def test_gather_second_reply_in_a_pipeline():
    r = hpredis.Reader(highway_mode=True)
    r.feed(arr([bulk(b"ab"), bulk(b"cd")]) + arr([bulk(b"ef")]))
    t1, a1 = r.gets()
    t2, a2 = r.gets()
    assert bytes(hpredis.highway_gather(t1, a1)) == b"abcd"
    assert bytes(hpredis.highway_gather(t2, a2)) == b"ef"
