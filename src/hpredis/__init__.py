"""hpredis_vec — hiredis-py compatible Reader built on the Mojo core.

Mirrors `hiredis.Reader` semantics verified against hiredis 3.4.2:
- error replies return `replyError` instances (top-level and nested); a
  `replyError` *callable* that raises (redis-py's `parse_error`) raises
- protocol errors raise `protocolError` and are sticky
- incomplete replies return `notEnoughData` (default False)
- `encoding` decodes bulk/simple strings and error messages
"""

import codecs

from . import hpredis_core as _core

_ERR_SENTINEL = b"\x00hpredis-error\x00"


class ProtocolError(Exception):
    pass


class ReplyError(Exception):
    pass


class PushNotification(list):
    """RESP3 push reply (hiredis-py PushNotification parity)."""


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
        self._pending = []
        self._pending_lengths = []
        self._pending_index = 0
        self._pending_count = 0
        if not callable(protocolError if protocolError is not None else ProtocolError):
            raise TypeError("protocolError must be callable")
        if not callable(replyError if replyError is not None else ReplyError):
            raise TypeError("replyError must be callable")
        if encoding is not None:
            codecs.lookup(encoding)  # LookupError, matches hiredis
        if errors is not None:
            codecs.lookup_error(errors)
        self._protocolError = protocolError if protocolError is not None else ProtocolError
        self._replyError = replyError if replyError is not None else ReplyError
        self._encoding = encoding
        self._errors = errors
        self._notEnoughData = notEnoughData
        self._highway = highway_mode
        self._maxbuf = 0
        self._closed = False
        self._dec_errors = None
        self._sync_decoding()

    def feed(self, data, start=None, stop=None):
        """Feed bytes/bytearray/memoryview. (data, start, stop) slices a buffer."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("feed() expects a buffer-protocol object")
        if start is not None or stop is not None:
            # hiredis feed(data, start, length): third arg is a LENGTH
            n = len(data)
            if start < 0 or start > n:
                raise ValueError("invalid offset")
            if stop is None:
                stop = n - start  # offset-only form: to the end
            if stop < 0:
                raise ValueError("invalid length")
            if start + stop > n:
                raise ValueError("invalid length")
            if not (start == 0 and stop == n):
                # whole-buffer feeds (redis-py: feed(buf, 0, n)) skip the
                # memoryview objects entirely
                data = memoryview(data)[start : start + stop]
        self._core.feed(data)

    def gets(self, should_decode=True):
        """Get one reply. The flag is hiredis-py's `shouldDecode`: it
        defaults to True and only decodes when an encoding is configured
        (redis-py's disable_decoding path calls gets(False))."""
        if self._highway:
            return self._highway_gets()
        if self._pending_index >= self._pending_count and self._encoding is None:
            prefetched = self._core.prefetch_clean()
            if type(prefetched) is not tuple:
                return prefetched
            if len(prefetched) == 3:
                _, self._pending, self._pending_lengths = prefetched
                self._pending_count = len(self._pending)
                self._pending_index = 0
            elif prefetched[0] == 1:
                return self._notEnoughData
        if self._pending_index < self._pending_count:
            index = self._pending_index
            reply = self._pending[index]
            self._pending_index = index + 1
            if self._pending_index == self._pending_count:
                self._clear_pending()
            if self._encoding is not None and should_decode:
                return self._finalize(reply, should_decode)
            return reply
        result = self._core.try_gets(1 if should_decode else 0)
        return self._handle_gets_result(result, should_decode)

    def _handle_gets_result(self, result, should_decode):
        if type(result) is not tuple:
            return result
        status, payload = result
        if status == 1:
            return self._notEnoughData
        if status == 6:
            # error markers inside the reply: _finalize builds replyError
            # instances (strings are already decoded)
            return self._finalize(payload, should_decode)
        if status == 7:
            # a leaf failed strict decoding: re-decode here so Python raises
            # the real codec error
            self._raise_decode_error(payload)
            return self._finalize(payload, should_decode)
        if status == 2:
            # hiredis always hands error text over as str, whatever
            # should_decode says (reader.c passes it through `"s"`)
            raise self._protocolError(self._decode_msg(payload))
        if status == 3:
            return self._replyError(self._decode_msg(payload[1]))
        if status == 4:
            return PushNotification(payload)
        raise TypeError("unhashable type in map reply")  # status 5

    def _clear_pending(self):
        self._pending = []
        self._pending_lengths = []
        self._pending_index = 0
        self._pending_count = 0

    def drain(self, should_decode=True):
        """Parse every complete reply already buffered and return them as a
        list, in one core call.  A `while r.gets() is not False: ...` loop
        pays the Mojo<->CPython boundary per reply; drain pays it once.

        Stops at an incomplete tail, which stays buffered for the next feed.
        A malformed reply raises protocolError and is sticky, as with gets().
        Not available in highway mode (drain may compact the shared buffer).
        """
        if self._highway:
            raise RuntimeError("drain() is not available in highway mode")
        pending_replies = self._pending[self._pending_index : self._pending_count]
        if pending_replies:
            self._clear_pending()
        replies, proto_msg, dict_err, had_markers, dec_failed, raise_exc = self._core.drain(
            1 if should_decode else 0, self._replyError
        )
        if dict_err:
            raise TypeError("unhashable type in map reply")
        if proto_msg is not None:
            raise self._protocolError(self._decode_msg(proto_msg))
        if dec_failed:
            self._raise_decode_error(replies)
        if raise_exc is not None:
            # the core captured the original exception from replyError
            raise raise_exc
        if pending_replies:
            replies = pending_replies + replies
            if had_markers or (self._encoding is not None and should_decode):
                replies = [self._finalize(r, should_decode) for r in replies]
        elif had_markers:
            # nested markers (an array holding an error) are the only case that
            # still needs the Python walk
            replies = [self._finalize(r, should_decode) for r in replies]
        return replies

    def _highway_gets(self):
        """Highway mode returns (table, arena) for a complete reply.

        table is n*24 bytes of little-endian int64 triples
        (offset, length, resp_type) with offsets into arena, which is the
        bytearray holding the reply.  Read the table in one call:
            triples = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
        then read payloads directly from arena:
            off, length, typ = triples[i]
            arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
        A view of the arena owns its data: it stays valid across feeds (the
        core moves to a fresh arena instead of resizing an exported one).
        """
        status, table, arena = self._core.highway_gets()
        if status == 1:
            return self._notEnoughData
        if status == 2:
            raise self._protocolError(self._decode_msg(table))
        return (table, arena)

    def set_encoding(self, encoding=None, errors=None):
        """hiredis parity: change encoding/errors; validates eagerly."""
        if encoding is not None:
            codecs.lookup(encoding)  # LookupError for unknown encodings
        if errors is not None:
            codecs.lookup_error(errors)  # LookupError for unknown handlers
        self._encoding = encoding
        self._errors = errors
        self._sync_decoding()

    def setmaxbuf(self, value):
        """Store hiredis' max-buffer setting; enforcement is not implemented."""
        if value is None:
            value = 0  # default: unlimited
        if not isinstance(value, int):
            raise TypeError("setmaxbuf() expects an int or None")
        if value < 0:
            raise ValueError("maxbuf must be >= 0")
        self._maxbuf = value

    def getmaxbuf(self):
        return self._maxbuf

    def len(self):
        return sum(self._pending_lengths[self._pending_index : self._pending_count]) + self._core.buffered()

    def __len__(self):
        return sum(self._pending_lengths[self._pending_index : self._pending_count]) + self._core.buffered()

    def has_data(self):
        """redis-py 8 can_read() support."""
        return self._pending_index < self._pending_count or self._core.buffered() > 0

    def close(self):
        """Release the core's arena now instead of at garbage collection."""
        self._clear_pending()
        core = getattr(self, "_core", None)
        if core is not None and not getattr(self, "_closed", False):
            self._closed = True
            core.free()

    def __del__(self):
        # the arena is raw C memory the Mojo struct does not own: without this
        # every collected Reader leaks its buffer (measured +250MB/2000 readers)
        self.close()

    def _sync_decoding(self):
        """Push the codec configuration into the core, which decodes string
        leaves during the parse.  These str objects stay on self so the C
        strings the core cached (PyUnicode_AsUTF8) remain valid."""
        if self._encoding is None:
            self._core.clear_decoding()
        else:
            self._dec_errors = self._errors if self._errors is not None else "strict"
            self._core.set_decoding(self._encoding, self._dec_errors)

    def _raise_decode_error(self, obj):
        """A leaf failed strict decoding in the core; the undecoded bytes
        were kept, so re-decoding here raises the real UnicodeDecodeError."""
        if isinstance(obj, bytes):
            obj.decode(self._encoding, self._errors or "strict")  # raises
        elif isinstance(obj, list):
            for item in obj:
                self._raise_decode_error(item)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                self._raise_decode_error(key)
                self._raise_decode_error(value)

    def _decode_msg(self, msg):
        if isinstance(msg, str):
            return msg
        # hiredis decodes error text as utf-8/"replace" whatever the
        # configured encoding is (reader.c PyUnicode_DecodeUTF8)
        return msg.decode("utf-8", "replace")

    def _finalize(self, obj, should_decode):
        if isinstance(obj, tuple) and obj and obj[0] == _ERR_SENTINEL:
            return self._replyError(self._decode_msg(obj[1]))
        if isinstance(obj, list):
            return [self._finalize(x, should_decode) for x in obj]
        if (
            isinstance(obj, bytes)
            and self._encoding is not None
            and should_decode
        ):
            return obj.decode(self._encoding, self._errors or "strict")
        return obj


def pack_command(args):
    """hiredis parity: serialize a command into RESP bytes."""
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        if isinstance(arg, bool):
            raise TypeError(f"invalid command argument type: {type(arg)!r}")
        if isinstance(arg, str):
            arg = arg.encode()
        elif isinstance(arg, (bytes, bytearray, memoryview)):
            arg = bytes(arg)
        elif isinstance(arg, (int, float)):
            arg = str(arg).encode()
        else:
            raise TypeError(f"invalid command argument type: {type(arg)!r}")
        parts.append(b"$%d\r\n" % len(arg) + arg + b"\r\n")
    return b"".join(parts)
