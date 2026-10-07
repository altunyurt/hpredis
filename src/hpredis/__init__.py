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
        status, payload = self._core.try_gets(1 if should_decode else 0)
        if status == 0:
            # string leaves were decoded in the core when an encoding is set
            return payload
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
        replies, proto_msg, dict_err, had_markers, dec_failed = self._core.drain(
            1 if should_decode else 0
        )
        if dict_err:
            raise TypeError("unhashable type in map reply")
        if proto_msg is not None:
            raise self._protocolError(self._decode_msg(proto_msg))
        if dec_failed:
            self._raise_decode_error(replies)
        if had_markers:
            # only marker conversion needs Python now; the common path returns
            # the core's list untouched
            replies = [self._finalize(r, should_decode) for r in replies]
        return replies

    def _highway_gets(self):
        status, addr, count = self._core.highway_gets()
        if status == 1:
            return self._notEnoughData
        if status != 0:
            raise self._protocolError("Protocol error")
        return (addr, count)

    def highway_slice(self, i):
        return self._core.highway_slice(i)

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
        """hiredis parity: max buffer guard. Value stored; enforcement is
        phase-5-polish (the hiredis suite does not exercise it)."""
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
        return self._core.buffered()

    def __len__(self):
        return self._core.buffered()

    def has_data(self):
        """redis-py 8 can_read() support."""
        return self._core.buffered() > 0

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


# Phase 4: zero-copy memoryview over highway slices (see phase spec).
def _memoryview_method(self, i):
    return self._core.memoryview(i)


Reader.memoryview = _memoryview_method


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
