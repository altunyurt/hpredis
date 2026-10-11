"""hpredis as a drop-in hiredis replacement.

Run: python examples/drop_in_hiredis.py

Two ways in: swap the import, or let redis-py use hpredis as its parser.
"""

import hpredis

# 1) The Reader interface is the same as hiredis-py.
r = hpredis.Reader()
r.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
assert r.gets() == [b"hello", b"world"]

# 2) redis-py reads the hiredis module's Reader attribute when it builds a
#    connection parser, so point it at hpredis before the first connection.
import hiredis  # noqa: E402

hiredis.Reader = hpredis.Reader

import fakeredis  # noqa: E402  in-process Redis, no server needed

client = fakeredis.FakeRedis()
client.rpush("queue", "a", "b", "c")
assert client.lrange("queue", 0, -1) == [b"a", b"b", b"c"]
pipe = client.pipeline(transaction=False)
pipe.set("k", 5)
pipe.get("k")
assert pipe.execute() == [True, b"5"]

# 3) asyncio clients use the same parser slot: redis.asyncio picks the
#    hiredis parser and constructs hiredis.Reader per connection.
import asyncio  # noqa: E402


async def async_main():
    client = fakeredis.aioredis.FakeRedis()
    await client.set("k", "v")
    assert await client.get("k") == b"v"
    await client.aclose()


asyncio.run(async_main())

print("drop-in OK: hiredis.Reader is", hiredis.Reader.__module__)
