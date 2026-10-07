"""hpredis_vec — hiredis-py compatible Reader built on the Mojo core.

Mirrors `hiredis.Reader` semantics verified against hiredis 3.4.2:
- error replies return `replyError` instances (top-level and nested); a
  `replyError` *callable* that raises (redis-py's `parse_error`) raises
- protocol errors raise `protocolError` and are sticky
- incomplete replies return `notEnoughData` (default False)
- `encoding` decodes bulk/simple strings and error messages
"""

from . import hpredis_core as _core

_ERR_SENTINEL = b"\x00hpredis-error\x00"


class ProtocolError(Exception):
    pass


class ReplyError(Exception):
    pass


class Reader:
    def __init__(
        self,
        protocolError=None,
        replyError=None,
        encoding=None,
        errors=None,
        notEnoughData=False,
        highway_mode=False,
    ):
        self._core = _core.Reader()
        self._protocolError = protocolError if protocolError is not None else ProtocolError
        self._replyError = replyError if replyError is not None else ReplyError
        self._encoding = encoding
        self._errors = errors
        self._notEnoughData = notEnoughData
        self._highway = highway_mode

    def feed(self, data, start=None, stop=None):
        """Feed bytes/bytearray/memoryview. (data, start, stop) slices a buffer."""
        if start is not None or stop is not None:
            data = memoryview(data)[start:stop]
        self._core.feed(data)

    def gets(self, disable_decoding=False):
        """Get one reply. See module docstring for semantics."""
        if self._highway:
            return self._highway_gets()
        status, payload = self._core.try_gets()
        if status == 1:
            return self._notEnoughData
        if status == 2:
            raise self._protocolError(self._decode_msg(payload, disable_decoding))
        if status == 3:
            return self._replyError(self._decode_msg(payload[1], disable_decoding))
        return self._finalize(payload, disable_decoding)

    def _highway_gets(self):
        status, addr, count = self._core.highway_gets()
        if status == 1:
            return self._notEnoughData
        if status != 0:
            raise self._protocolError("Protocol error")
        return (addr, count)

    def highway_slice(self, i):
        return self._core.highway_slice(i)

    def has_data(self):
        """redis-py 8 can_read() support."""
        return self._core.buffered() > 0

    def _decode_msg(self, msg, disable_decoding):
        if disable_decoding or isinstance(msg, str):
            return msg
        # hiredis always decodes error messages with errors="replace",
        # regardless of the configured encoding/errors (verified 3.4.2)
        return msg.decode(self._encoding or "utf-8", "replace")

    def _finalize(self, obj, disable_decoding):
        if isinstance(obj, tuple) and obj and obj[0] == _ERR_SENTINEL:
            return self._replyError(self._decode_msg(obj[1], disable_decoding))
        if isinstance(obj, list):
            return [self._finalize(x, disable_decoding) for x in obj]
        if (
            isinstance(obj, bytes)
            and self._encoding is not None
            and not disable_decoding
        ):
            return obj.decode(self._encoding, self._errors or "strict")
        return obj


# Phase 4: zero-copy memoryview over highway slices (see phase spec).
def _memoryview_method(self, i):
    return self._core.memoryview(i)


Reader.memoryview = _memoryview_method
