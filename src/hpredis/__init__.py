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
        # the core returns this object from gets() on a partial reply, so the
        # incomplete path never re-enters Python
        self._core.set_not_enough_data(notEnoughData)
        # highway routing lives in the core so gets() is one native call
        self._core.set_highway_mode(1 if highway_mode else 0)
        # weak: the core is owned by this wrapper, so a strong ref to the
        # handler would be an uncollectable cycle
        self._core.set_wrapper(weakref.WeakMethod(self._handle_gets_result))
        protocol_error = protocolError if protocolError is not None else ProtocolError
        reply_error = replyError if replyError is not None else ReplyError
        if not callable(protocol_error):
            raise TypeError("protocolError must be callable")
        if not callable(reply_error):
            raise TypeError("replyError must be callable")
        if encoding is not None:
            self._encoding_name = codecs.lookup(encoding).name  # LookupError
        else:
            self._encoding_name = None
        if errors is not None:
            codecs.lookup_error(errors)
        self._protocolError = protocol_error
        self._replyError = reply_error
        self._encoding = encoding
        self._errors = errors
        self._notEnoughData = notEnoughData
        self._highway = highway_mode
        self._maxbuf = 16384  # hiredis' default
        self._dec_errors = None
        self._sync_decoding()
        if isinstance(reply_error, type):
            # classes are safe to cache strongly; a strong cache of a bound
            # method or closure would keep reader/callable cycles alive
            self._core.set_reply_error(reply_error, False)
        else:
            if getattr(reply_error, "__self__", None) is not None:
                re_get = weakref.WeakMethod(reply_error)
            else:
                try:
                    re_get = weakref.ref(reply_error)
                except TypeError:
                    # callables without weakref support: strong proxy getter
                    re_get = lambda: reply_error
            self._core.set_reply_error(re_get, True)

    def __getattr__(self, name):
        """Route ``gets`` to the core.

        The core owns the reply: clean replies, the ``notEnoughData``
        sentinel and highway results all come back without a Python frame.
        Only rare outcomes (protocol errors, pushes, nested markers, decode
        failures) call ``_handle_gets_result`` back, through the WeakMethod
        registered in ``__init__``.  The bound core method is cached in the
        instance dict, so this runs once per Reader.
        """
        if name == "gets" or name == "feed":
            # bound to the core object, which holds the state; cached in the
            # instance dict so the lookup happens once per Reader
            fn = getattr(self._core, name)
            self.__dict__[name] = fn
            return fn
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}")

    def _handle_gets_result(self, result, should_decode):
        # indexing, not unpacking: highway protocol errors arrive as
        # (status, message, None) so they cannot be confused with a
        # (table, arena) result
        status = result[0]
        payload = result[1]
        if status == 1:
            # only reachable when no notEnoughData object was registered
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

    def drain(self, should_decode=True, max_replies=0):
        """Parse every complete reply already buffered and return them as a
        list, in one core call.  `gets()` is now a native call per reply, but
        drain still wins on large batches: it compacts the arena once and
        builds one result list instead of one Python object chain per reply.

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
        replies, proto_msg, dict_exc, had_markers, dec_failed, raise_exc = self._core.drain(
            should_decode, max_replies
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
        if had_markers:
            # nested markers (an array holding an error) are the only case that
            # still needs the Python walk
            replies = [self._finalize(r, should_decode) for r in replies]
        return replies

    def set_encoding(self, encoding=None, errors=None):
        """hiredis parity: change encoding/errors; validates eagerly."""
        if encoding is not None:
            self._encoding_name = codecs.lookup(encoding).name  # LookupError
        else:
            self._encoding_name = None
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
        return self._core.buffered()

    def has_data(self):
        """redis-py 8 can_read() support."""
        return self._core.buffered() > 0

    def close(self):
        """Release the core's arena now instead of at garbage collection.

        free() is idempotent, so a closed reader can be fed again and closed
        again without tracking state here (feed lives in the core).
        """
        core = getattr(self, "_core", None)
        if core is not None:
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
            name = self._encoding_name
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
