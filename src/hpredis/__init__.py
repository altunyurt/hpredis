"""hpredis_vec — hiredis-py compatible Reader built on the Mojo core.

Mirrors `hiredis.Reader` semantics verified against hiredis 3.4.2:
- error replies return `replyError` instances (top-level and nested); a
  `replyError` *callable* that raises (redis-py's `parse_error`) raises
- protocol errors raise `protocolError` and are sticky
- incomplete replies return `notEnoughData` (default False)
- `encoding` decodes bulk/simple strings and error messages
"""

import codecs
import weakref

from . import hpredis_core as _core

_ERR_SENTINEL = b"\x00hpredis-error\x00"
_PUSH_SENTINEL = b"\x00hpredis-push\x00"

__all__ = [
    "HiredisError",
    "ProtocolError",
    "ReplyError",
    "PushNotification",
    "Reader",
    "pack_command",
]
# The hiredis-py API version this module emulates.  redis-py gates on this
# value (redis/utils.py requires >= 3.2), so it describes the emulated API,
# not this package's own release number.
__version__ = "3.4.2"


class HiredisError(Exception):
    """Base class for reader errors (hiredis-py parity)."""


class ProtocolError(HiredisError):
    pass


class ReplyError(HiredisError):
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
        self._feed = self._core.feed
        self._prefetch = self._core.prefetch_clean
        self._try_gets = self._core.try_gets
        self._pending = []
        self._pending_lengths = []
        self._pending_index = 0
        self._pending_count = 0
        self._pending_bytes = 0
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
        self._maxbuf = 16384  # hiredis' default
        self._closed = False
        self._dec_errors = None
        # True when the core reported no complete reply; no bytes can appear
        # before the next feed(), so gets()/has_data() can answer without a
        # core call (redis-py polls both between reads)
        self._exhausted = True
        # a non-clean reply head was seen: skip the prefetch probe until a
        # clean reply or the next feed (error-heavy streams otherwise pay a
        # wasted core call per reply)
        self._special = False
        self._sync_decoding()
        re = self._replyError
        if isinstance(re, type):
            # classes are safe to cache strongly; a strong cache of a bound
            # method or closure would keep reader/callable cycles alive
            self._reply_get = re
            self._core.set_reply_error(re, False)
        else:
            if getattr(re, "__self__", None) is not None:
                re_get = weakref.WeakMethod(re)
            else:
                try:
                    re_get = weakref.ref(re)
                except TypeError:
                    # callables without weakref support: strong proxy getter
                    re_get = lambda: re
            self._reply_get = re_get
            self._core.set_reply_error(re_get, True)

    def feed(self, data, start=None, stop=None):
        """Feed any buffer-protocol object; (start, stop) is a hiredis-style
        (offset, length) window."""
        if start is not None or stop is not None:
            # hiredis feed(data, start, length): third arg is a LENGTH
            view = memoryview(data)  # TypeError for non-buffer objects
            n = len(view)
            if start is None:
                start = 0
            if start < 0 or start > n:
                raise ValueError("invalid offset")
            if stop is None:
                stop = n - start  # offset-only form: to the end
            if stop < 0:
                raise ValueError("invalid length")
            if start + stop > n:
                raise ValueError("invalid length")
            if not (start == 0 and stop == n):
                # the whole-buffer case (redis-py: feed(buf, 0, n)) stays on
                # the fast path and skips the memoryview object entirely
                data = view[start : start + stop]
        if self._feed(data) != 0:
            raise TypeError("a bytes-like object is required")
        self._exhausted = False
        self._special = False
        self._closed = False

    def gets(self, should_decode=True):
        """Get one reply. The flag is hiredis-py's `shouldDecode`: it
        defaults to True and only decodes when an encoding is configured
        (redis-py's disable_decoding path calls gets(False))."""
        if self._highway:
            return self._highway_gets()
        if self._exhausted and self._pending_index >= self._pending_count:
            return self._notEnoughData
        if (
            self._pending_index >= self._pending_count
            and self._encoding is None
            and not self._special
        ):
            prefetched = self._prefetch()
            if type(prefetched) is not tuple:
                return prefetched
            if len(prefetched) == 3:
                _, self._pending, self._pending_lengths = prefetched
                self._pending_count = len(self._pending)
                self._pending_index = 0
                self._pending_bytes = sum(self._pending_lengths)
            elif prefetched[0] == 1:
                self._exhausted = True
                return self._notEnoughData
            elif prefetched[0] == 10:
                # lone reply that drained the buffer: the poll before the next
                # socket read answers here instead of crossing into the core
                self._exhausted = True
                return prefetched[1]
            else:
                self._special = True
        if self._pending_index < self._pending_count:
            index = self._pending_index
            reply = self._pending[index]
            self._pending_index = index + 1
            self._pending_bytes -= self._pending_lengths[index]
            if self._pending_index == self._pending_count:
                self._clear_pending()
            if self._encoding is not None and should_decode:
                return self._finalize(reply, should_decode)
            return reply
        result = self._try_gets(1 if should_decode else 0)
        if type(result) is not tuple:
            self._special = False
            return result
        return self._handle_gets_result(result, should_decode)

    def _handle_gets_result(self, result, should_decode):
        if type(result) is not tuple:
            return result
        status, payload = result
        if status == 1:
            self._exhausted = True
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
            # pushes can carry nested error markers too
            return PushNotification(self._finalize(payload, should_decode))
        if isinstance(payload, BaseException):
            raise payload  # CPython's TypeError from the dict insert
        raise TypeError("unhashable type in map reply")  # status 5

    def _clear_pending(self):
        self._pending = []
        self._pending_lengths = []
        self._pending_index = 0
        self._pending_count = 0
        self._pending_bytes = 0

    def drain(self, should_decode=True, max_replies=0):
        """Parse every complete reply already buffered and return them as a
        list, in one core call.  A `while r.gets() is not False: ...` loop
        pays the Mojo<->CPython boundary per reply; drain pays it once.

        max_replies > 0 caps the batch size (the rest stays buffered), so a
        100 MB buffer does not have to become one list.
        Stops at an incomplete tail, which stays buffered for the next feed.
        A malformed reply raises protocolError and is sticky, as with gets().
        Not available in highway mode (drain may compact the shared buffer).
        """
        if self._highway:
            raise RuntimeError("drain() is not available in highway mode")
        if not isinstance(max_replies, int) or max_replies < 0:
            raise ValueError("max_replies must be a non-negative int")
        pending_replies = self._pending[self._pending_index : self._pending_count]
        if pending_replies:
            self._clear_pending()
        replies, proto_msg, dict_exc, had_markers, dec_failed, raise_exc = self._core.drain(
            1 if should_decode else 0, max_replies
        )
        if dict_exc is not None:
            raise dict_exc
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
        bytearray holding the reply.  Rows are pre-order: byte payloads are
        slices, nil is length -1, bool carries its 't'/'f' byte, and
        containers emit a header row (length = child count) before their
        children.  Read the table in one call:
            triples = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
        then read payloads directly from arena:
            off, length, typ = triples[0]  # single bulk: one payload row
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
        """hiredis' maxbuf: gates idle free-space trimming, not input.

        hiredis does not use it to reject large replies (verified 3.4.2), and
        neither do we; the value sets the arena size kept after a fully
        consumed batch.  0 disables trimming.
        """
        if value is None:
            value = 16384  # hiredis resets to its default buffer size
        if not isinstance(value, int):
            raise TypeError("setmaxbuf() expects an int or None")
        if value < 0:
            raise ValueError("maxbuf must be >= 0")
        self._maxbuf = value
        self._core.set_maxbuf(value)

    def getmaxbuf(self):
        return self._maxbuf

    def len(self):
        return self._pending_bytes + self._core.buffered()

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
        # release the arena and cached refs; the wrapper may outlive an older
        # core build that has no dispose(), so fall back to free() and never
        # let a finalizer raise
        core = getattr(self, "_core", None)
        if core is None:
            return
        try:
            release = getattr(core, "dispose", None)
            if release is not None:
                release()
            else:
                core.free()
        except Exception:
            pass

    def _sync_decoding(self):
        """Push the codec configuration into the core, which decodes string
        leaves during the parse.  These str objects stay on self so the C
        strings the core cached (PyUnicode_AsUTF8) remain valid."""
        if self._encoding is None:
            self._core.clear_decoding()
        else:
            self._dec_errors = self._errors if self._errors is not None else "strict"
            name = codecs.lookup(self._encoding).name
            if name == "utf-8":
                kind = 1
            elif name in ("latin-1", "iso8859-1"):
                kind = 2
            elif name == "ascii":
                kind = 3
            else:
                kind = 0  # generic PyUnicode_Decode with the codec lookup
            self._core.set_decoding(self._encoding, self._dec_errors, kind)

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
        if isinstance(obj, tuple) and obj and obj[0] == _PUSH_SENTINEL:
            return PushNotification(self._finalize(obj[1], should_decode))
        if isinstance(obj, list):
            return [self._finalize(x, should_decode) for x in obj]
        if isinstance(obj, dict):
            return {
                self._finalize(k, should_decode): self._finalize(v, should_decode)
                for k, v in obj.items()
            }
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
        parts.append(b"$%d\r\n" % len(arg))
        parts.append(arg)
        parts.append(b"\r\n")
    return b"".join(parts)
