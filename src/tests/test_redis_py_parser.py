"""redis-py's hiredis parser paths, driven with hpredis.

fakeredis overrides ``read_response`` and hands back pre-built objects, so it
never runs the parser.  These tests drive redis-py's real sync and async
hiredis parsers over fake sockets instead: the parser constructs
``hiredis.Reader`` per connection, and the patch below makes that hpredis.
"""

import asyncio

import hiredis
import pytest

import hpredis


class _Encoder:
    decode_responses = False
    encoding = "utf-8"
    encoding_errors = "strict"


class _FakeSock:
    def __init__(self, chunks):
        self.chunks = list(chunks)

    def settimeout(self, timeout):
        pass

    def recv_into(self, buf):
        if not self.chunks:
            return 0
        data = self.chunks.pop(0)
        buf[: len(data)] = data
        return len(data)


class _FakeConn:
    socket_timeout = None
    encoder = _Encoder()

    def __init__(self, sock):
        self._sock = sock


class _FakeAsyncConn:
    encoder = _Encoder()

    def __init__(self, stream):
        self._reader = stream


@pytest.fixture()
def patched_hiredis():
    original = hiredis.Reader
    hiredis.Reader = hpredis.Reader
    try:
        yield
    finally:
        hiredis.Reader = original


def test_sync_hiredis_parser_resumes_across_reads(patched_hiredis):
    from redis._parsers.hiredis import _HiredisParser

    parser = _HiredisParser(socket_read_size=65536)
    parser.on_connect(_FakeConn(_FakeSock(
        [b"*2\r\n$5\r\nhel", b"lo\r\n$5\r\nworld\r\n"])))
    assert isinstance(parser._reader, hpredis.Reader)
    assert parser.read_response() == [b"hello", b"world"]

    # has_data() is what can_read() uses to skip a socket read
    parser._reader.feed(b"+OK\r\n")
    assert parser.can_read() is True
    assert parser.read_response() == b"OK"


def test_async_hiredis_parser_resumes_across_reads(patched_hiredis):
    from redis._parsers.hiredis import _AsyncHiredisParser

    async def run():
        stream = asyncio.StreamReader()
        parser = _AsyncHiredisParser(socket_read_size=65536)
        parser.on_connect(_FakeAsyncConn(stream))
        assert isinstance(parser._reader, hpredis.Reader)

        stream.feed_data(b"*2\r\n$5\r\nhel")
        response = asyncio.ensure_future(parser.read_response())
        await asyncio.sleep(0)  # let it drain the first chunk
        stream.feed_data(b"lo\r\n$5\r\nworld\r\n")
        assert await response == [b"hello", b"world"]

        stream.feed_data(b"+OK\r\n")
        assert await parser.read_response() == b"OK"
        stream.feed_eof()

    asyncio.run(run())
