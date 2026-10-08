"""Differential fuzzing against hiredis-py plus a redis-py command sweep.

hiredis-py is the oracle: every generated payload is fed to both readers and
the normalized outcomes must match exactly. The sweep routes redis-py through
hpredis.Reader with fakeredis as a real in-memory RESP server.
"""

import random

import pytest

import hpredis


def _sanitize(data: bytes) -> bytes:
    return data.replace(b"\r", b"x").replace(b"\n", b"y")


def rand_resp(rng, depth=0, max_depth=4):
    """Generate a random RESP reply. Returns (payload, python_value)."""
    kinds = range(6) if depth < max_depth else range(5)
    kind = rng.choice(list(kinds))
    if kind == 0:  # simple string
        s = _sanitize(rng.randbytes(rng.randrange(0, 24)))
        return b"+" + s + b"\r\n", s
    if kind == 1:  # error
        s = _sanitize(rng.randbytes(rng.randrange(0, 24)))
        return b"-" + s + b"\r\n", ("ERR", s)
    if kind == 2:  # integer
        v = rng.randrange(-10**6, 10**6)
        return b":%d\r\n" % v, v
    if kind == 3:  # bulk
        if rng.randrange(10) == 0:
            return b"$-1\r\n", None
        data = rng.randbytes(rng.randrange(0, 48))
        return b"$%d\r\n" % len(data) + data + b"\r\n", data
    if kind == 4:  # null array
        return b"*-1\r\n", None
    # array
    n = rng.randrange(0, 5)
    parts = []
    values = []
    for _ in range(n):
        p, v = rand_resp(rng, depth + 1, max_depth)
        parts.append(p)
        values.append(v)
    return b"*%d\r\n" % n + b"".join(parts), values


def mutate(rng, payload: bytes):
    """Corrupt a payload: truncate, flip a byte, or replace the lead byte."""
    if not payload:
        return payload
    mode = rng.randrange(3)
    if mode == 0 and len(payload) > 1:
        return payload[: rng.randrange(1, len(payload))]
    if mode == 1:
        b = bytearray(payload)
        b[rng.randrange(len(b))] = rng.randrange(256)
        return bytes(b)
    return rng.randbytes(1) + payload[1:]


def norm(x):
    if isinstance(x, list):
        return [norm(i) for i in x]
    if isinstance(x, Exception):
        return (
            "EXC",
            type(x).__name__,
            tuple(a if isinstance(a, (bytes, str, int)) else repr(a) for a in x.args),
        )
    if isinstance(x, tuple) and x and x[0] == b"\x00hpredis-error\x00":
        return ("ERR", x[1])
    return x


def drain_both(hr, mine):
    """Repeatedly gets() both readers; return list of normalized outcomes."""
    out = []
    for _ in range(32):
        try:
            a = hr.gets()
        except Exception as e:  # noqa: BLE001
            a = e
        try:
            b = mine.gets()
        except Exception as e:  # noqa: BLE001
            b = e
        a, b = norm(a), norm(b)
        if a is False and b is False:
            break
        out.append((a, b))
    return out


def test_fuzz_parity_seeded():
    import hiredis

    rng = random.Random(12345)
    for i in range(300):
        payload, _ = rand_resp(rng)
        if rng.randrange(4) == 0:
            payload = mutate(rng, payload)
        hr, mine = hiredis.Reader(), hpredis.Reader()
        hr.feed(payload)
        mine.feed(payload)
        for a, b in drain_both(hr, mine):
            assert a == b, f"iter {i} payload {payload!r}\n  hiredis: {a!r}\n  ours:    {b!r}"


def test_fuzz_fragmented_seeded():
    import hiredis

    rng = random.Random(777)
    for i in range(150):
        payload, _ = rand_resp(rng, max_depth=3)
        hr, mine = hiredis.Reader(), hpredis.Reader()
        pos = 0
        while pos < len(payload):
            n = rng.randrange(1, 9)
            chunk = payload[pos : pos + n]
            hr.feed(chunk)
            mine.feed(chunk)
            pos += n
            for a, b in drain_both(hr, mine):
                assert a == b, (
                    f"iter {i} chunk@{pos} payload {payload!r}\n"
                    f"  hiredis: {a!r}\n  ours:    {b!r}"
                )


def test_fuzz_multiple_replies_one_feed():
    import hiredis

    rng = random.Random(4242)
    for i in range(100):
        parts = [rand_resp(rng)[0] for _ in range(rng.randrange(1, 4))]
        payload = b"".join(parts)
        hr, mine = hiredis.Reader(), hpredis.Reader()
        hr.feed(payload)
        mine.feed(payload)
        for a, b in drain_both(hr, mine):
            assert a == b, f"iter {i} payload {payload!r}\n  hiredis: {a!r}\n  ours:    {b!r}"


def test_deep_nesting_parity():
    import hiredis

    depth = 50
    payload = b"*1\r\n" * depth + b":1\r\n"
    hr, mine = hiredis.Reader(), hpredis.Reader()
    hr.feed(payload)
    mine.feed(payload)
    assert norm(hr.gets()) == norm(mine.gets())


def test_multimegabyte_bulk_parity():
    import hiredis

    big = bytes(range(256)) * (5 * 1024 * 1024 // 256)  # 5 MB
    payload = b"$%d\r\n" % len(big) + big + b"\r\n"
    hr, mine = hiredis.Reader(), hpredis.Reader()
    hr.feed(payload)
    mine.feed(payload)
    assert hr.gets() == mine.gets() == big


@pytest.fixture()
def redis_client():
    import fakeredis  # noqa: PLC0415
    import hiredis  # noqa: PLC0415

    original = hiredis.Reader
    hiredis.Reader = hpredis.Reader
    try:
        yield fakeredis.FakeRedis()
    finally:
        hiredis.Reader = original


def test_redis_py_command_sweep(redis_client):
    assert redis_client.set("counter", 6) is True
    assert redis_client.incr("counter") == 7
    assert redis_client.hset("h", "k", "v") == 1
    assert redis_client.hget("h", "k") == b"v"
    assert redis_client.lrange("l", 0, -1) == []
    redis_client.rpush("l", "a", "b")
    assert redis_client.lrange("l", 0, -1) == [b"a", b"b"]
    pipe = redis_client.pipeline(transaction=False)
    pipe.set("k", 5)
    pipe.get("k")
    assert pipe.execute() == [True, b"5"]
