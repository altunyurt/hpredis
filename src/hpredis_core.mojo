# Phase 4: AI Highway — Mojo core (phase 3 core + memoryview + compaction-safe highway).
# Compile: mojo build phase4.mojo --emit shared-lib -o hpredis_core.so

from std.python import Python, PythonObject
from std.python.python_object import PyObjectPtr
from std.python.bindings import PythonModuleBuilder
from std.os import abort
from std.ffi import external_call, c_int, c_ssize_t, c_size_t
from std.collections import List
from std.origin import MutAnyOrigin, MutUntrackedOrigin
from std.builtin.value import Defaultable
from std.format import Writable

# RESP leading bytes
comptime TYPE_ARRAY = 42  # '*'
comptime TYPE_BULK = 36  # '$'
comptime TYPE_SIMPLE = 43  # '+'
comptime TYPE_ERROR = 45  # '-'
comptime TYPE_INT = 58  # ':'

# try_gets status codes
comptime ST_OK = 0
comptime ST_INCOMPLETE = 1
comptime ST_PROTO_ERR = 2
comptime ST_REPLY_ERR = 3
comptime ST_PUSH = 4
comptime ST_DICT_ERR = 5
# clean reply that contains nested error markers: the wrapper must run
# _finalize to build replyError instances
comptime ST_OK_MARKERS = 6
# a leaf failed strict decoding: the wrapper re-raises the real codec error
comptime ST_DECODE_ERR = 7
# private prefetch outcomes
comptime ST_PREFETCH_SPECIAL = 8
comptime ST_PREFETCH_BATCH = 9

# ponytail: cap batches between replies at 64 KiB / 4096 items; one oversized
# scalar is unavoidable. Raise the cap only if profiling justifies it.
comptime PREFETCH_MAX_REPLIES = 4096
comptime PREFETCH_MAX_BYTES = 65536

# nested error marker sentinel (tuple[0]); unique, never valid RESP data
comptime ERR_SENTINEL = "\x00hpredis-error\x00"
# nested push reply sentinel; the wrapper rebuilds a PushNotification
comptime PUSH_SENTINEL = "\x00hpredis-push\x00"


struct ResponseSlice(Movable, ImplicitlyCopyable, Writable):
    var offset: Int
    var length: Int
    var resp_type: UInt8

    def __init__(out self):
        self.offset = 0
        self.length = 0
        self.resp_type = 0

    def write_to(self, mut writer: Some[Writer]):
        "slice".write_to(writer)

    def write_repr_to(self, mut writer: Some[Writer]):
        t"ResponseSlice({self.offset}, {self.length}, {self.resp_type})".write_to(writer)


struct Node(Movable):
    var status: UInt8
    var pos: Int
    var payload: PythonObject
    var err_msg: String
    # True when the wrapper must finalize the payload: a nested error reply
    # or a nested push (both carry marker tuples)
    var had_err: Bool

    def __init__(out self, status: UInt8, pos: Int, payload: PythonObject):
        self.status = status
        self.pos = pos
        self.payload = payload
        self.err_msg = String()
        self.had_err = False

    def __init__(out self, status: UInt8, pos: Int, payload: PythonObject, err_msg: String):
        self.status = status
        self.pos = pos
        self.payload = payload
        self.err_msg = err_msg
        self.had_err = False


struct Reader(Defaultable, Movable, Writable):
    var buf_addr: Int
    var buf_cap: Int
    var buf_len: Int
    var consumed: Int
    var proto_err: Bool
    var proto_err_msg: String
    # decoding configuration (C string addresses owned by the wrapper)
    var dec_enabled: Bool
    var dec_encoding: Int
    var dec_errors: Int
    var dec_kind: UInt8
    # the wrapper's replyError callable, cached once (see set_reply_error)
    var reply_error: Int
    var reply_error_weak: Bool
    # hiredis-compatible maxbuf: gates idle free-space trimming, not input
    var maxbuf: Int
    # one cached (ST_INCOMPLETE, None) tuple: polls must not allocate
    var incomplete_tuple: Int
    # a build attempt hit an incomplete reply: scan before rebuilding
    var needs_scan: Bool
    # ponytail: resume between top-level collection children; an incomplete
    # nested child is rescanned until complete, add a scan stack if measured.
    var scan_active: Bool
    var scan_pos: Int
    var scan_remaining: Int
    # arena as a Python bytearray: views over it keep it alive, and CPython
    # refuses to resize an exported buffer (see _ensure_cap)
    var buf_obj: Int
    var highway_slices: List[ResponseSlice]

    def __init__(out self):
        self.buf_addr = 0
        self.buf_cap = 0
        self.buf_len = 0
        self.consumed = 0
        self.proto_err = False
        self.proto_err_msg = String()
        self.dec_enabled = False
        self.dec_encoding = 0
        self.dec_errors = 0
        self.dec_kind = 0
        self.reply_error = 0
        self.reply_error_weak = False
        self.maxbuf = 16384
        self.incomplete_tuple = 0
        self.needs_scan = False
        self.scan_active = False
        self.scan_pos = 0
        self.scan_remaining = 0
        self.buf_obj = 0
        self.highway_slices = List[ResponseSlice]()

    # === Python-facing methods ===

    @staticmethod
    def feed(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], data: PythonObject) raises -> PythonObject:
        """Copy a buffer-protocol object (bytes/bytearray/memoryview) into the arena."""
        ref cpy = Python().cpython()
        var raw = data.steal_data()  # steal_data detaches the ref: release it below
        var view = PyBuffer()
        var rc = external_call["PyObject_GetBuffer", c_int](
            raw, Pointer(to=view), c_int(0))  # PyBUF_SIMPLE
        if rc != 0:
            _ = cpy.Py_DecRef(raw)
            _ = cpy.PyErr_Clear()  # the wrapper raises TypeError
            return PythonObject(1)
        var n = Int(view.len)
        if n > 0:
            if _arena_pinned(self_ptr[]):
                _arena_detach(self_ptr[], self_ptr[].buf_cap)
            _ensure_cap(self_ptr[], self_ptr[].buf_len + n)
            var dst = Pointer[UInt8, MutAnyOrigin](
                unsafe_from_address=self_ptr[].buf_addr + self_ptr[].buf_len)
            _ = external_call["memcpy", Pointer[UInt8, MutAnyOrigin]](
                dst, view.buf.value(), c_size_t(n))
            self_ptr[].buf_len += n
        _ = external_call["PyBuffer_Release", NoneType](Pointer(to=view))
        _ = cpy.Py_DecRef(raw)
        return PythonObject(0)

    @staticmethod
    def set_decoding(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], encoding: PythonObject, errors: PythonObject, kind: PythonObject) raises -> PythonObject:
        """Cache codec C strings for in-core decoding.  PyUnicode_AsUTF8
        points into the str objects, which the wrapper keeps alive."""
        var enc_obj = encoding.steal_data()
        var errs_obj = errors.steal_data()
        var enc = external_call["PyUnicode_AsUTF8", Pointer[UInt8, MutAnyOrigin]](enc_obj)
        var errs = external_call["PyUnicode_AsUTF8", Pointer[UInt8, MutAnyOrigin]](errs_obj)
        # the wrapper keeps the str objects alive for the cached C strings
        _ = Python().cpython().Py_DecRef(enc_obj)
        _ = Python().cpython().Py_DecRef(errs_obj)
        self_ptr[].dec_encoding = Int(enc)
        self_ptr[].dec_errors = Int(errs)
        self_ptr[].dec_kind = UInt8(Int(py=kind))
        self_ptr[].dec_enabled = True
        return PythonObject(0)

    @staticmethod
    def clear_decoding(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        self_ptr[].dec_enabled = False
        self_ptr[].dec_encoding = 0
        self_ptr[].dec_errors = 0
        self_ptr[].dec_kind = 0
        return PythonObject(0)

    @staticmethod
    def set_maxbuf(
        self_ptr: Pointer[mut=True, Self, MutAnyOrigin], value: PythonObject
    ) raises -> PythonObject:
        """Store maxbuf (the wrapper validates it); 0 disables trimming."""
        self_ptr[].maxbuf = Int(py=value)
        return PythonObject(0)

    @staticmethod
    def set_reply_error(
        self_ptr: Pointer[mut=True, Self, MutAnyOrigin],
        reply_error: PythonObject,
        is_weak: PythonObject,
    ) raises -> PythonObject:
        """Cache the wrapper's replyError factory once per Reader.

        Classes are stored directly (is_weak false); anything else is a
        zero-arg weakref getter, so reader/callable cycles stay collectable.
        """
        ref cpy = Python().cpython()
        if self_ptr[].reply_error != 0:
            _ = cpy.Py_DecRef(_int_ptr(self_ptr[].reply_error))
        self_ptr[].reply_error = Int(reply_error.steal_data())
        self_ptr[].reply_error_weak = Int(py=is_weak) != 0
        return PythonObject(0)

    @staticmethod
    def try_gets(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], should_decode: PythonObject) raises -> PythonObject:
        if self_ptr[].proto_err:
            # sticky protocol error (matches hiredis): keep raising
            return _status_tuple(ST_PROTO_ERR, _bytes_payload(self_ptr[].proto_err_msg))
        if self_ptr[].consumed >= self_ptr[].buf_len:
            return Reader._incomplete_result(self_ptr)
        var sd = Int(py=should_decode)
        var cnv = DecodeCtx(
            enabled=self_ptr[].dec_enabled and sd != 0,
            encoding=self_ptr[].dec_encoding,
            errors=self_ptr[].dec_errors,
            kind=self_ptr[].dec_kind,
            failed=False)
        var ptr = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_addr)
        if self_ptr[].needs_scan:
            # a previous chunk was incomplete: rebuild only once a scan says
            # the whole reply is buffered (chunked replies are otherwise
            # rebuilt from scratch on every chunk)
            if _scan_needs(self_ptr[], ptr) == ST_INCOMPLETE:
                return Reader._incomplete_result(self_ptr)
        if ptr.unsafe_offset(self_ptr[].consumed)[] == 45 and self_ptr[].reply_error != 0:
            # top-level error replies are the hottest non-clean path: call the
            # replyError factory straight from C like hiredis does, instead of
            # building a marker tuple and a second status tuple
            var crlf = _find_crlf(ptr, self_ptr[].consumed + 1, self_ptr[].buf_len)
            if crlf < 0:
                self_ptr[].needs_scan = True
                return Reader._incomplete_result(self_ptr)
            var msg_start = self_ptr[].consumed + 1
            var msg_len = crlf - msg_start
            var inst = _error_to_instance(
                self_ptr[].reply_error, self_ptr[].reply_error_weak,
                ptr.unsafe_offset(msg_start), msg_len)
            if inst != 0:
                self_ptr[].consumed = crlf + 2
                self_ptr[].needs_scan = False
                _ = _compact(self_ptr[])
                return PythonObject(from_owned=PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](
                    unsafe_from_address=inst)))
            if Int(external_call["PyErr_Occurred", PyObjectPtr]()) != 0:
                # the factory raised: consume the reply and let the pending
                # CPython exception propagate with its original traceback
                self_ptr[].consumed = crlf + 2
                self_ptr[].needs_scan = False
                return PythonObject(from_owned=PyObjectPtr())
            # dead getter: hand the marker to the wrapper's finalizer instead
            var marker = _error_marker(ptr.unsafe_offset(msg_start), msg_len)
            self_ptr[].consumed = crlf + 2
            self_ptr[].needs_scan = False
            _ = _compact(self_ptr[])
            return _status_tuple(ST_REPLY_ERR, marker)
        var node = _parse_node(ptr, self_ptr[].consumed, self_ptr[].buf_len, 1, cnv)
        if node.status == ST_INCOMPLETE:
            self_ptr[].needs_scan = True
            return Reader._incomplete_result(self_ptr)
        self_ptr[].needs_scan = False
        if node.status == ST_OK or node.status == ST_PUSH:
            self_ptr[].consumed = node.pos
            _ = _compact(self_ptr[])
            if cnv.failed:
                return _status_tuple(ST_DECODE_ERR, node.payload.steal_data())
            if node.status == ST_OK and node.had_err:
                return _status_tuple(ST_OK_MARKERS, node.payload.steal_data())
            if node.status == ST_OK:
                return PythonObject(from_owned=node.payload.steal_data())
            return _status_tuple(ST_PUSH, node.payload.steal_data())
        if node.status == ST_REPLY_ERR:
            # consumed (matches hiredis); wrapper builds the replyError instance
            self_ptr[].consumed = node.pos
            _ = _compact(self_ptr[])
            return _status_tuple(ST_REPLY_ERR, node.payload.steal_data())
        if node.status == ST_DICT_ERR:
            self_ptr[].consumed = node.pos
            _ = _compact(self_ptr[])
            return _status_tuple(ST_DICT_ERR, node.payload.steal_data())
        # protocol error: sticky, not consumed
        self_ptr[].proto_err = True
        self_ptr[].proto_err_msg = node.err_msg
        return _status_tuple(ST_PROTO_ERR, node.payload.steal_data())

    @staticmethod
    def prefetch_clean(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        """Batch clean scalar replies; return a lone reply directly.

        Containers stay for try_gets.  Error replies are batched too when the
        cached factory is a class (no raise path), so error-heavy streams pay
        one core call per batch instead of one per reply; a custom/raising
        callable stays special.  Used only when decoding is disabled.  Batched
        replies include wire lengths so the wrapper can preserve len().
        """
        var collected = List[PyObjectPtr]()
        var lengths = List[PyObjectPtr]()
        var first_payload = PyObjectPtr()
        var first_length = 0
        var reply_count = 0
        var batch_bytes = 0
        var special = False
        ref cpy = Python().cpython()
        if self_ptr[].proto_err:
            return _status_tuple(ST_PREFETCH_SPECIAL, _none_payload())
        if self_ptr[].consumed >= self_ptr[].buf_len:
            return Reader._incomplete_result(self_ptr)
        var ptr = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_addr)
        var cnv = DecodeCtx(
            enabled=False,
            encoding=self_ptr[].dec_encoding,
            errors=self_ptr[].dec_errors,
            kind=self_ptr[].dec_kind,
            failed=False)
        while reply_count < PREFETCH_MAX_REPLIES and batch_bytes < PREFETCH_MAX_BYTES:
            if self_ptr[].consumed >= self_ptr[].buf_len:
                break
            if self_ptr[].needs_scan:
                var scan_status = _scan_needs(self_ptr[], ptr)
                if scan_status == ST_INCOMPLETE:
                    break
                self_ptr[].needs_scan = False
                if scan_status == ST_PROTO_ERR:
                    special = True
                    break
            var pos = self_ptr[].consumed
            var t = ptr.unsafe_offset(pos)[]
            var payload = PyObjectPtr()
            var wire_len = 0
            if t == TYPE_ERROR:
                # classes never raise from construction; batch them so
                # error-heavy streams pay one core call per batch
                if self_ptr[].reply_error == 0 or self_ptr[].reply_error_weak:
                    special = True
                    break
                var crlf = _find_crlf(ptr, pos + 1, self_ptr[].buf_len)
                if crlf < 0:
                    self_ptr[].needs_scan = True
                    break
                self_ptr[].needs_scan = False
                var inst = _error_to_instance(
                    self_ptr[].reply_error, False, ptr.unsafe_offset(pos + 1),
                    crlf - (pos + 1))
                if inst == 0:
                    if Int(external_call["PyErr_Occurred", PyObjectPtr]()) != 0:
                        _ = cpy.PyErr_Clear()
                    special = True
                    break
                payload = PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](
                    unsafe_from_address=inst))
                wire_len = crlf + 2 - pos
                self_ptr[].consumed = crlf + 2
            else:
                # Containers and pushes have wrapper-specific finalization.
                if t == TYPE_ARRAY or t == 62 or t == 37 or t == 126:
                    special = True
                    break
                var node = _parse_node(ptr, pos, self_ptr[].buf_len, 1, cnv)
                if node.status == ST_INCOMPLETE:
                    self_ptr[].needs_scan = True
                    break
                self_ptr[].needs_scan = False
                if node.status != ST_OK or node.had_err or cnv.failed:
                    special = True
                    break
                wire_len = node.pos - pos
                self_ptr[].consumed = node.pos
                payload = node.payload.steal_data()
            if reply_count == 0:
                first_payload = payload
                first_length = wire_len
            else:
                if reply_count == 1:
                    collected.append(first_payload)
                    lengths.append(cpy.PyLong_FromSsize_t(first_length))
                collected.append(payload)
                lengths.append(cpy.PyLong_FromSsize_t(wire_len))
            reply_count += 1
            batch_bytes += wire_len
        _ = _compact(self_ptr[])
        if reply_count == 0:
            if special:
                return _status_tuple(ST_PREFETCH_SPECIAL, _none_payload())
            return Reader._incomplete_result(self_ptr)
        if reply_count == 1:
            return PythonObject(from_owned=first_payload)
        return _prefetch_batch_result(_list_of(collected), _list_of(lengths))

    @staticmethod
    def _incomplete_result(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        """Return the cached (ST_INCOMPLETE, None) tuple, incref'd per caller."""
        if self_ptr[].incomplete_tuple == 0:
            self_ptr[].incomplete_tuple = Int(
                _status_tuple(ST_INCOMPLETE, _none_payload()).steal_data())
        var t = _int_ptr(self_ptr[].incomplete_tuple)
        _ = Python().cpython().Py_IncRef(t)
        return PythonObject(from_owned=t)

    @staticmethod
    def dispose(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        """Final release: buffers plus the cached Python references."""
        _ = Reader.free(self_ptr)
        ref cpy = Python().cpython()
        if self_ptr[].reply_error != 0:
            _ = cpy.Py_DecRef(_int_ptr(self_ptr[].reply_error))
            self_ptr[].reply_error = 0
        if self_ptr[].incomplete_tuple != 0:
            _ = cpy.Py_DecRef(_int_ptr(self_ptr[].incomplete_tuple))
            self_ptr[].incomplete_tuple = 0
        return PythonObject(0)

    @staticmethod
    def free(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        """Release the arena.  The wrapper calls this from __del__: without it
        every Reader leaks its buffer when it is garbage collected (measured:
        +250MB over 2000 readers fed 64KB each)."""
        if self_ptr[].buf_obj != 0:
            _ = external_call["Py_DecRef", NoneType](
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_obj))
            self_ptr[].buf_obj = 0
            self_ptr[].buf_addr = 0
            self_ptr[].buf_cap = 0
            self_ptr[].buf_len = 0
            self_ptr[].consumed = 0
        self_ptr[].needs_scan = False
        self_ptr[].scan_active = False
        self_ptr[].scan_pos = 0
        self_ptr[].scan_remaining = 0
        if self_ptr[].reply_error != 0:
            _ = Python().cpython().Py_DecRef(_int_ptr(self_ptr[].reply_error))
            self_ptr[].reply_error = 0
        return PythonObject(0)

    @staticmethod
    def drain(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], should_decode: PythonObject, max_replies: PythonObject) raises -> PythonObject:
        """Parse every complete reply currently buffered, into one list.

        Top-level error replies become replyError instances here: the wrapper's
        Python walk over the result cost ~140ns per reply.  Returns (replies,
        proto_msg, dict_err, nested_markers, dec_failed, raise_exc); raise_exc
        is set when the callable raised; the wrapper raises that same exception
        object, preserving its traceback without calling the callable again.
        max_replies > 0 stops the batch early; the rest stays buffered.
        """
        var max_n = Int(py=max_replies)
        var collected = List[PyObjectPtr]()
        var sd = Int(py=should_decode)
        var re_ptr = self_ptr[].reply_error
        var raise_exc = _none_payload()
        var cnv = DecodeCtx(
            enabled=self_ptr[].dec_enabled and sd != 0,
            encoding=self_ptr[].dec_encoding,
            errors=self_ptr[].dec_errors,
            kind=self_ptr[].dec_kind,
            failed=False)
        if self_ptr[].proto_err:
            return _drain_result(
                _list_of(collected), _bytes_payload(self_ptr[].proto_err_msg), _none_payload(),
                False, False, _none_payload())
        if self_ptr[].consumed < self_ptr[].buf_len:
            var ptr = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_addr)
            var had_markers = False
            while True:
                if self_ptr[].consumed >= self_ptr[].buf_len:
                    break
                if max_n > 0 and len(collected) >= max_n:
                    break
                if self_ptr[].needs_scan:
                    if _scan_needs(self_ptr[], ptr) == ST_INCOMPLETE:
                        break
                var node = _parse_node(ptr, self_ptr[].consumed, self_ptr[].buf_len, 1, cnv)
                if node.status == ST_INCOMPLETE:
                    self_ptr[].needs_scan = True
                    break
                self_ptr[].needs_scan = False
                if node.status == ST_DICT_ERR:
                    if len(collected) > 0:
                        # deliver what parsed; the next drain()/gets() raises
                        break
                    # unhashable map key: consumed; the wrapper raises TypeError
                    self_ptr[].consumed = node.pos
                    _ = _compact(self_ptr[])
                    return _drain_result(
                        _list_of(collected), _none_payload(), node.payload.steal_data(),
                        False, cnv.failed, _none_payload())
                if node.status == ST_PROTO_ERR:
                    if len(collected) > 0:
                        # leave it pending for the next drain()/gets()
                        break
                    # sticky and not consumed (matches gets)
                    self_ptr[].proto_err = True
                    self_ptr[].proto_err_msg = node.err_msg
                    _ = _compact(self_ptr[])
                    return _drain_result(
                        _list_of(collected), _bytes_payload(node.err_msg), _none_payload(),
                        False, cnv.failed, _none_payload())
                if node.status == ST_REPLY_ERR and re_ptr != 0:
                    var marker = Int(node.payload.steal_data())
                    var callback_error = PyObjectPtr()
                    var inst = _marker_to_instance(re_ptr, self_ptr[].reply_error_weak, marker, callback_error)
                    if inst != 0:
                        self_ptr[].consumed = node.pos
                        collected.append(PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](
                            unsafe_from_address=inst)))
                        _ = external_call["Py_DecRef", NoneType](
                            Pointer[UInt8, MutAnyOrigin](unsafe_from_address=marker))
                        continue
                    if Int(callback_error) != 0:
                        if len(collected) > 0:
                            # deliver the replies parsed so far; the next
                            # drain()/gets() raises on this error reply
                            _ = external_call["Py_DecRef", NoneType](
                                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(callback_error)))
                            _ = external_call["Py_DecRef", NoneType](
                                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=marker))
                            break
                        # Preserve the exception from the one callback invocation;
                        # the wrapper raises this object without calling again.
                        self_ptr[].consumed = node.pos
                        raise_exc = callback_error
                        _ = external_call["Py_DecRef", NoneType](
                            Pointer[UInt8, MutAnyOrigin](unsafe_from_address=marker))
                        break
                    # No exception object was available: leave the marker for
                    # the wrapper's normal finalization path.
                    had_markers = True
                    collected.append(PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](
                        unsafe_from_address=marker)))
                    continue
                if cnv.failed:
                    if len(collected) > 0:
                        # stop before the reply whose leaf failed to decode
                        cnv.failed = False
                        break
                    self_ptr[].consumed = node.pos
                    collected.append(node.payload.steal_data())
                    break
                if (node.status == ST_OK and node.had_err) or node.status == ST_PUSH:
                    # nested markers/pushes still need the wrapper's walk
                    had_markers = True
                self_ptr[].consumed = node.pos
                if node.status == ST_PUSH:
                    collected.append(_push_marker(node.payload.steal_data()))
                else:
                    collected.append(node.payload.steal_data())
            # compact once for the whole batch (gets compacts per reply)
            _ = _compact(self_ptr[])
            return _drain_result(
                _list_of(collected), _none_payload(), _none_payload(), had_markers,
                cnv.failed, raise_exc)
        return _drain_result(
            _list_of(collected), _none_payload(), _none_payload(), False, cnv.failed, _none_payload())

    @staticmethod
    def buffered(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        return PythonObject(self_ptr[].buf_len - self_ptr[].consumed)

    @staticmethod
    def highway_gets(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        """One call per reply: (status, table, arena).

        status 0: table is n*24 bytes of little-endian int64 triples
        (offset, length, resp_type) with absolute offsets into arena, and arena
        is the bytearray holding the reply.  Rows are pre-order and cover
        every item: byte payloads carry a slice, nil carries length -1, bool
        carries its 't'/'f' byte, and a container emits a header row
        (length = child count) before its children.  Consumers read payloads
        with np.frombuffer(arena, offset=..., count=...) / memoryview: no
        per-slice bridge call, and the view owns the buffer.
        status 1: incomplete -> (1, None, None).
        status 2: protocol error (sticky, like gets) -> (2, message, None).
        """
        ref cpy = Python().cpython()
        if self_ptr[].consumed >= self_ptr[].buf_len:
            return _hw_tuple(cpy.PyLong_FromSsize_t(ST_INCOMPLETE), _none_payload(), _none_payload())
        var ptr = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_addr)
        var start = self_ptr[].consumed
        self_ptr[].highway_slices.clear()
        var res = _scan_highway(ptr, start, self_ptr[].buf_len, self_ptr[].highway_slices, start, 1)
        if res.status == ST_INCOMPLETE:
            return _hw_tuple(cpy.PyLong_FromSsize_t(ST_INCOMPLETE), _none_payload(), _none_payload())
        if res.status == ST_PROTO_ERR:
            self_ptr[].proto_err = True
            self_ptr[].proto_err_msg = res.err_msg
            return _hw_tuple(
                cpy.PyLong_FromSsize_t(ST_PROTO_ERR), _bytes_payload(res.err_msg), _none_payload())
        self_ptr[].consumed = res.pos
        # Bound the arena like the classic path does, but only when no view is
        # alive: an exported bytearray must not be rewritten under its readers.
        if self_ptr[].consumed == self_ptr[].buf_len and not _arena_pinned(self_ptr[]):
            self_ptr[].buf_len = 0
            self_ptr[].consumed = 0
        var n = len(self_ptr[].highway_slices)
        var table = _new_arena(n * 24, 0, 0)
        var tp = _arena_ptr(table)
        for i in range(n):
            var s = self_ptr[].highway_slices[i]
            _write_i64(tp, i * 24, start + s.offset)
            _write_i64(tp, i * 24 + 8, s.length)
            _write_i64(tp, i * 24 + 16, Int(s.resp_type))
        var arena = PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](
            unsafe_from_address=self_ptr[].buf_obj))
        _ = cpy.Py_IncRef(arena)
        return _hw_tuple(
            cpy.PyLong_FromSsize_t(ST_OK),
            PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](
                unsafe_from_address=table)),
            arena)

    def write_to(self, mut writer: Some[Writer]):
        t"Reader(buffered={self.buf_len - self.consumed})".write_to(writer)

    def write_repr_to(self, mut writer: Some[Writer]):
        t"Reader(buffered={self.buf_len - self.consumed})".write_to(writer)


# === CPython buffer protocol (declared locally; not in std.python._cpython) ===

struct PyBuffer(Defaultable):
    var buf: OptionalPointer[UInt8, MutUntrackedOrigin]
    var obj: PyObjectPtr
    var len: c_ssize_t
    var itemsize: c_ssize_t
    var readonly: c_int
    var ndim: c_int
    var format: OptionalPointer[UInt8, MutUntrackedOrigin]
    var shape: OptionalPointer[c_ssize_t, MutUntrackedOrigin]
    var strides: OptionalPointer[c_ssize_t, MutUntrackedOrigin]
    var suboffsets: OptionalPointer[c_ssize_t, MutUntrackedOrigin]
    var internal: OptionalPointer[UInt8, MutUntrackedOrigin]

    def __init__(out self):
        self.buf = {}
        self.obj = PyObjectPtr()
        self.len = 0
        self.itemsize = 0
        self.readonly = 0
        self.ndim = 0
        self.format = {}
        self.shape = {}
        self.strides = {}
        self.suboffsets = {}
        self.internal = {}


# === Arena ===

def _new_arena(size: Int, src: Int, src_len: Int) raises -> Int:
    """A bytearray of `size` bytes holding a copy of src_len bytes at src.

    PyByteArray_FromStringAndSize(NULL, n) needs a null pointer, which Mojo's
    non-nullable Pointer cannot express; a zero-length copy followed by
    PyByteArray_Resize has the same effect and never reads src.
    """
    var obj = external_call["PyByteArray_FromStringAndSize", PyObjectPtr](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=src), c_ssize_t(src_len))
    if Int(obj) == 0:
        raise Error("out of memory")
    if size > src_len:
        var rc = external_call["PyByteArray_Resize", c_int](
            Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(obj)), c_ssize_t(size))
        if rc != 0:
            _ = Python().cpython().PyErr_Clear()
            raise Error("out of memory")
    return Int(obj)


def _arena_ptr(obj: Int) -> Pointer[UInt8, MutAnyOrigin]:
    return external_call["PyByteArray_AsString", Pointer[UInt8, MutAnyOrigin]](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=obj))


def _arena_pinned(r: Reader) -> Bool:
    """True when the arena is referenced outside this Reader.

    That covers memoryviews/numpy arrays *and* the bare bytearray handed to
    highway_gets() callers.  ob_refcnt sits at offset 0 of every PyObject on
    standard (GIL) CPython builds; this field is the single baseline
    reference.  ob_exports (offset 48 of PyByteArrayObject on 3.9-3.13) is
    kept as a belt-and-braces check for exported buffers.
    """
    if r.buf_obj == 0:
        return False
    var refcnt = Int(Pointer[Int, MutAnyOrigin](unsafe_from_address=r.buf_obj)[])
    if refcnt > 1:
        return True
    return Int(Pointer[Int, MutAnyOrigin](unsafe_from_address=r.buf_obj + 48)[]) > 0


def _arena_detach(mut r: Reader, want_cap: Int) raises:
    """Continue in a fresh arena, copying the pending bytes over.

    Called when a feed meets an exported buffer: a held memoryview/numpy array
    points into the old bytearray, so reusing that memory (even without a
    resize question, since the old bytes would be overwritten in place) would
    silently rewrite its data.  The view keeps the old bytearray alive and
    parsing continues in the new one.
    """
    var pending = r.buf_len - r.consumed
    var cap = want_cap if want_cap > pending else pending
    var fresh = _new_arena(cap, r.buf_addr + r.consumed, pending)
    _ = external_call["Py_DecRef", NoneType](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_obj))
    r.buf_obj = fresh
    r.buf_cap = cap
    r.buf_len = pending
    r.consumed = 0
    r.scan_active = False
    r.scan_pos = 0
    r.scan_remaining = 0
    r.buf_addr = Int(_arena_ptr(r.buf_obj))


def _ensure_cap(mut r: Reader, needed: Int) raises:
    if needed <= r.buf_cap:
        return
    var new_cap = r.buf_cap if r.buf_cap > 0 else 4096
    while new_cap < needed:
        new_cap *= 2
    if r.buf_obj == 0:
        r.buf_obj = _new_arena(new_cap, r.buf_addr, 0)
        r.buf_cap = new_cap
        r.buf_addr = Int(_arena_ptr(r.buf_obj))
        return
    if _arena_pinned(r):
        # a live view pins the old arena: continue in a fresh one
        _arena_detach(r, new_cap)
        return
    var rc = external_call["PyByteArray_Resize", c_int](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_obj), c_ssize_t(new_cap))
    if rc != 0:
        _ = Python().cpython().PyErr_Clear()
        _arena_detach(r, new_cap)
        return
    r.buf_cap = new_cap
    r.buf_addr = Int(_arena_ptr(r.buf_obj))


def _compact(mut r: Reader) -> Bool:
    """Returns True if a memmove compaction happened."""
    if r.consumed == 0:
        return False
    if r.consumed == r.buf_len:
        r.buf_len = 0
        r.consumed = 0
        r.scan_active = False
        r.scan_pos = 0
        r.scan_remaining = 0
        # released an oversized arena instead of pinning it for the life of
        # the reader (hiredis trims an empty buffer's free space above maxbuf)
        if r.maxbuf > 0 and r.buf_cap > r.maxbuf and not _arena_pinned(r):
            var rc = external_call["PyByteArray_Resize", c_int](
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_obj), c_ssize_t(r.maxbuf))
            if rc == 0:
                r.buf_cap = r.maxbuf
                r.buf_addr = Int(_arena_ptr(r.buf_obj))
            else:
                _ = Python().cpython().PyErr_Clear()
        return False
    if r.consumed * 2 >= r.buf_len and r.buf_len > 1024:
        var dst = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_addr)
        var src = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_addr + r.consumed)
        _ = external_call["memmove", Pointer[UInt8, MutAnyOrigin]](
            dst, src, c_size_t(r.buf_len - r.consumed))
        if r.scan_active:
            r.scan_pos -= r.consumed
        r.buf_len -= r.consumed
        r.consumed = 0
        return True
    return False


# === Result helpers ===

def _int_ptr(value: Int) -> PyObjectPtr:
    """Wrap a raw PyObject* address as a non-owning PyObjectPtr."""
    return PyObjectPtr(upcast_from=Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=value))


def _none_payload() -> PyObjectPtr:
    # callers wrap this with from_owned, so hand over an owned reference
    ref cpy = Python().cpython()
    var none = cpy.Py_None()
    _ = cpy.Py_IncRef(none)
    return none


def _bytes_payload(s: String) raises -> PyObjectPtr:
    # String utf8 bytes via PyBytes
    return external_call["PyBytes_FromStringAndSize", PyObjectPtr](
        s.unsafe_ptr(), c_ssize_t(s.byte_length()))


struct DecodeCtx(ImplicitlyCopyable):
    """Decoding state threaded through a parse.  encoding/errors are C string
    addresses owned by the wrapper's str objects (they outlive the parse);
    kind selects the specialized decode entry point (1 utf-8, 2 latin-1,
    3 ascii, 0 generic _codecs lookup)."""
    var enabled: Bool
    var encoding: Int
    var errors: Int
    var kind: UInt8
    var failed: Bool

    def __init__(out self, enabled: Bool, encoding: Int, errors: Int, kind: UInt8, failed: Bool):
        self.enabled = enabled
        self.encoding = encoding
        self.errors = errors
        self.kind = kind
        self.failed = failed


def _leaf_payload(
    ptr: Pointer[UInt8, MutAnyOrigin], offset: Int, length: Int, mut cnv: DecodeCtx
) raises -> PyObjectPtr:
    """String leaf: decoded in the core when an encoding is configured, so
    decoding costs no extra Python pass (hiredis decodes the same way)."""
    if cnv.enabled:
        var res = PyObjectPtr()
        if cnv.kind == 1:
            res = external_call["PyUnicode_DecodeUTF8", PyObjectPtr](
                ptr.unsafe_offset(offset), c_ssize_t(length),
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=cnv.errors))
        elif cnv.kind == 2:
            res = external_call["PyUnicode_DecodeLatin1", PyObjectPtr](
                ptr.unsafe_offset(offset), c_ssize_t(length),
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=cnv.errors))
        elif cnv.kind == 3:
            res = external_call["PyUnicode_DecodeASCII", PyObjectPtr](
                ptr.unsafe_offset(offset), c_ssize_t(length),
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=cnv.errors))
        else:
            res = external_call["PyUnicode_Decode", PyObjectPtr](
                ptr.unsafe_offset(offset), c_ssize_t(length),
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=cnv.encoding),
                Pointer[UInt8, MutAnyOrigin](unsafe_from_address=cnv.errors))
        if Int(res) != 0:
            return res
        # a Mojo raise would replace the pending codec error, so fall back to
        # bytes and let the wrapper re-raise it
        _ = Python().cpython().PyErr_Clear()
        cnv.failed = True
    return _bytes_slice_payload(ptr, offset, length)


def _bytes_slice_payload(ptr: Pointer[UInt8, MutAnyOrigin], offset: Int, length: Int) raises -> PyObjectPtr:
    return external_call["PyBytes_FromStringAndSize", PyObjectPtr](
        ptr.unsafe_offset(offset), c_ssize_t(length))


def _status_tuple(status: UInt8, payload: PyObjectPtr) raises -> PythonObject:
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(2)
    _ = cpy.PyTuple_SetItem(t, 0, cpy.PyLong_FromSsize_t(Int(status)))
    _ = cpy.PyTuple_SetItem(t, 1, payload)
    return PythonObject(from_owned=t)


def _write_i64(ptr: Pointer[UInt8, MutAnyOrigin], offset: Int, value: Int):
    """Little-endian int64 store (numpy '<i8' on the consumer side)."""
    var v = UInt64(value)
    for k in range(8):
        ptr.unsafe_offset(offset + k)[] = UInt8((v >> UInt64(8 * k)) & 0xFF)


def _hw_tuple(a: PyObjectPtr, b: PyObjectPtr, c: PyObjectPtr) raises -> PythonObject:
    """Tuple of three already-owned references (PyTuple_SetItem steals)."""
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(3)
    _ = cpy.PyTuple_SetItem(t, 0, a)
    _ = cpy.PyTuple_SetItem(t, 1, b)
    _ = cpy.PyTuple_SetItem(t, 2, c)
    return PythonObject(from_owned=t)


def _prefetch_batch_result(replies: PyObjectPtr, lengths: PyObjectPtr) raises -> PythonObject:
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(3)
    _ = cpy.PyTuple_SetItem(t, 0, cpy.PyLong_FromSsize_t(ST_PREFETCH_BATCH))
    _ = cpy.PyTuple_SetItem(t, 1, replies)
    _ = cpy.PyTuple_SetItem(t, 2, lengths)
    return PythonObject(from_owned=t)


def _drain_result(
    replies: PyObjectPtr, proto_msg: PyObjectPtr, dict_exc: PyObjectPtr, had_markers: Bool,
    dec_failed: Bool, raise_exc: PyObjectPtr
) raises -> PythonObject:
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(6)
    _ = cpy.PyTuple_SetItem(t, 0, replies)
    _ = cpy.PyTuple_SetItem(t, 1, proto_msg)
    _ = cpy.PyTuple_SetItem(t, 2, dict_exc)
    _ = cpy.PyTuple_SetItem(t, 3, cpy.PyBool_FromLong(1) if had_markers else cpy.PyBool_FromLong(0))
    _ = cpy.PyTuple_SetItem(t, 4, cpy.PyBool_FromLong(1) if dec_failed else cpy.PyBool_FromLong(0))
    _ = cpy.PyTuple_SetItem(t, 5, raise_exc)
    return PythonObject(from_owned=t)


def _list_of(imm collected: List[PyObjectPtr]) raises -> PyObjectPtr:
    """Python list from collected stolen refs (PyList_SetItem steals them).

    Building it in one shot beats PyList_Append per reply: the bound
    PyList_SetItem is cheap, while external_call marshalling per reply costs
    ~0.3ns per payload byte (measured).
    """
    ref cpy = Python().cpython()
    var lst = cpy.PyList_New(len(collected))
    for i in range(len(collected)):
        _ = cpy.PyList_SetItem(lst, i, collected[i])
    return lst


def _marker_to_instance(re_ptr: Int, weak: Bool, marker_ptr: Int, mut raised: PyObjectPtr) -> Int:
    """Turn an error marker tuple into a replyError instance in the core.

    Returns 0 and stores the raised exception in `raised` when the callable
    fails. PyErr_GetRaisedException clears the C error indicator while
    preserving the original exception and traceback for the Python wrapper.
    """
    ref cpy = Python().cpython()
    var msg = external_call["PyTuple_GetItem", PyObjectPtr](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=marker_ptr), c_ssize_t(1))
    if Int(msg) == 0:
        raised = external_call["PyErr_GetRaisedException", PyObjectPtr]()
        return 0
    # hiredis hands error text over as str, always (utf-8/"replace")
    var bp = external_call["PyBytes_AsString", Pointer[UInt8, MutAnyOrigin]](msg)
    var bn = Int(external_call["PyBytes_Size", c_ssize_t](msg))
    var owned = False
    var callable = _resolve_reply_error(re_ptr, weak, owned)
    if Int(callable) == 0:
        raised = external_call["PyErr_GetRaisedException", PyObjectPtr]()
        return 0
    if Int(callable) == Int(cpy.Py_None()):
        # dead weakref: drain falls back to the wrapper's own callable
        if owned:
            _ = cpy.Py_DecRef(callable)
        return 0
    var enc = String("utf-8")
    var errs = String("replace")
    var text = external_call["PyUnicode_Decode", PyObjectPtr](
        bp, c_ssize_t(bn),
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(enc.unsafe_ptr())),
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(errs.unsafe_ptr())))
    if Int(text) == 0:
        if owned:
            _ = cpy.Py_DecRef(callable)
        raised = external_call["PyErr_GetRaisedException", PyObjectPtr]()
        return 0
    var inst = external_call["PyObject_CallOneArg", PyObjectPtr](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(callable)),
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(text)))
    _ = cpy.Py_DecRef(text)
    if owned:
        _ = cpy.Py_DecRef(callable)
    if Int(inst) == 0:
        raised = external_call["PyErr_GetRaisedException", PyObjectPtr]()
        return 0
    return Int(inst)

def _resolve_reply_error(re_ptr: Int, weak: Bool, mut owned: Bool) -> PyObjectPtr:
    """Resolve the cached replyError factory.

    Classes are cached strongly (no GC cycle is possible); anything else is a
    zero-arg weakref getter.  `owned` says whether the caller must Py_DecRef
    the result.
    """
    if not weak:
        owned = False
        return _int_ptr(re_ptr)
    owned = True
    return external_call["PyObject_CallNoArgs", PyObjectPtr](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=re_ptr))


def _error_to_instance(
    re_ptr: Int, weak: Bool, msg_ptr: Pointer[UInt8, MutAnyOrigin], msg_len: Int
) -> Int:
    """Build a replyError instance straight from the error line bytes.

    Decodes utf-8/"replace" like hiredis and calls the factory once; no marker
    tuple, sentinel bytes or status tuple are allocated.  Returns 0 with the
    CPython exception left pending when the factory or a C call failed, so the
    caller can propagate it directly with its traceback.
    """
    ref cpy = Python().cpython()
    var enc = String("utf-8")
    var errs = String("replace")
    var text = external_call["PyUnicode_Decode", PyObjectPtr](
        msg_ptr, c_ssize_t(msg_len),
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(enc.unsafe_ptr())),
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(errs.unsafe_ptr())))
    if Int(text) == 0:
        return 0
    var owned = False
    var callable = _resolve_reply_error(re_ptr, weak, owned)
    if Int(callable) == 0:
        _ = cpy.Py_DecRef(text)
        return 0
    if Int(callable) == Int(cpy.Py_None()):
        _ = cpy.Py_DecRef(text)
        if owned:
            _ = cpy.Py_DecRef(callable)
        return 0
    var inst = external_call["PyObject_CallOneArg", PyObjectPtr](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(callable)),
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(text)))
    _ = cpy.Py_DecRef(text)
    if owned:
        _ = cpy.Py_DecRef(callable)
    if Int(inst) == 0:
        return 0
    return Int(inst)


def _push_marker(payload: PyObjectPtr) raises -> PyObjectPtr:
    """Nested push reply → (sentinel_bytes, payload); the wrapper wraps it."""
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(2)
    _ = cpy.PyTuple_SetItem(t, 0, _bytes_payload(String(PUSH_SENTINEL)))
    _ = cpy.PyTuple_SetItem(t, 1, payload)
    return t


def _error_marker(msg_ptr: Pointer[UInt8, MutAnyOrigin], length: Int) raises -> PyObjectPtr:
    """Nested error reply → (sentinel_bytes, message_bytes)."""
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(2)
    var sentinel = _bytes_payload(String(ERR_SENTINEL))
    _ = cpy.PyTuple_SetItem(t, 0, sentinel)
    _ = cpy.PyTuple_SetItem(t, 1, _bytes_slice_payload(msg_ptr, 0, length))
    return t


# === RESP parsing ===

def _bool_ok(bval: Int) -> Bool:
    """hiredis accepts t/f case-insensitively (#t/#T/#f/#F)."""
    return bval == 116 or bval == 102 or bval == 84 or bval == 70


def _is_inf_or_nan(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, crlf: Int) -> Bool:
    """Exactly inf/nan, case-insensitive (hiredis rejects 'infinity')."""
    if crlf - start != 3:
        return False
    var a = Int(ptr.unsafe_offset(start)[]) | 32
    var b = Int(ptr.unsafe_offset(start + 1)[]) | 32
    var c = Int(ptr.unsafe_offset(start + 2)[]) | 32
    if a == 105 and b == 110 and c == 102:  # inf
        return True
    return a == 110 and b == 97 and c == 110  # nan


def _double_chars_ok(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, crlf: Int) -> Bool:
    """hiredis' double grammar: optional '-', then digits/'.'/exponent
    characters, or exactly inf/nan.  Leading '+', hex floats and underscores
    are rejected before strtod runs (verified against hiredis 3.4.2)."""
    var i = start
    if i < crlf and ptr.unsafe_offset(i)[] == 45:  # '-'
        i += 1
    if i >= crlf:
        return False
    if _is_inf_or_nan(ptr, i, crlf):
        return True
    var first = Int(ptr.unsafe_offset(i)[])
    if not (48 <= first <= 57 or first == 46):
        return False
    while i < crlf:
        var ch = Int(ptr.unsafe_offset(i)[])
        if not (48 <= ch <= 57 or ch == 46 or ch == 101 or ch == 69 or ch == 43 or ch == 45):
            return False
        i += 1
    return True


def _bignum_ok(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, crlf: Int) -> Bool:
    """hiredis accepts an optional '-' followed by at least one digit."""
    var i = start
    if i < crlf and ptr.unsafe_offset(i)[] == 45:  # '-'
        i += 1
    if i >= crlf:
        return False
    while i < crlf:
        var c = Int(ptr.unsafe_offset(i)[])
        if c < 48 or c > 57:
            return False
        i += 1
    return True


def _find_crlf(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int) -> Int:
    var p = ptr.unsafe_offset(start)
    var remaining = end - start
    while remaining > 0:
        var hit = external_call["memchr", Pointer[UInt8, MutAnyOrigin]](
            p, c_int(13), c_size_t(remaining))
        if Int(hit) == 0:
            return -1
        var pos = Int(hit) - Int(ptr)
        if pos + 1 < end and ptr.unsafe_offset(pos + 1)[] == 10:
            return pos
        p = ptr.unsafe_offset(pos + 1)
        remaining = end - (pos + 1)
    return -1


def _read_int(
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int, mut out_end: Int, mut status: UInt8
) -> Int:
    """Find the CRLF, then parse the line (see _parse_int_line).

    status: 0 ok / 1 incomplete / 2 protocol error."""
    var crlf = _find_crlf(ptr, start, end)
    if crlf < 0:
        status = ST_INCOMPLETE
        return 0
    return _parse_int_line(ptr, start, crlf, out_end, status)


def _read_len_fast(
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int, mut out_end: Int, mut status: UInt8
) -> Int:
    """Positive length header in one pass (digits until CRLF, no memchr).

    Falls back to the verified strict parser for '-', leading zeros, junk and
    overflow, so semantics are unchanged for every non-hot input.
    """
    if start < end:
        var first = Int(ptr.unsafe_offset(start)[])
        if 49 <= first <= 57:
            var value = UInt64(first - 48)
            var i = start + 1
            while i < end:
                var d = Int(ptr.unsafe_offset(i)[])
                if d == 13:
                    if i + 1 >= end:
                        break
                    if ptr.unsafe_offset(i + 1)[] != 10:
                        break
                    out_end = i + 2
                    status = ST_OK
                    return Int(value)
                if not (48 <= d <= 57):
                    break
                if value > 1844674407370955161:
                    break
                value *= 10
                if value > 18446744073709551615 - UInt64(d - 48):
                    break
                value += UInt64(d - 48)
                i += 1
    return _read_int(ptr, start, end, out_end, status)


def _parse_int_line(
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, crlf: Int, mut out_end: Int, mut status: UInt8
) -> Int:
    """Parse the signed int in [start, crlf], replicating hiredis string2ll
    exactly (leading zeros rejected, unsigned 64-bit overflow math).

    Split from _read_int so callers that already located the CRLF (the ':' and
    '#' branches) do not scan the line twice.  status: 0 ok / 2 protocol error.
    """
    var line_len = crlf - start
    var i = start
    var neg = False
    if line_len > 0 and ptr.unsafe_offset(i)[] == 45:  # '-'
        neg = True
        i += 1
    if i >= start + line_len:
        status = ST_PROTO_ERR  # only a negative sign
        return 0
    var first = Int(ptr.unsafe_offset(i)[])
    if first == 48:  # '0': valid only as the entire line
        if line_len != 1:
            status = ST_PROTO_ERR
            return 0
        out_end = crlf + 2
        status = ST_OK
        return 0
    if not (49 <= first <= 57):
        status = ST_PROTO_ERR
        return 0
    var value = UInt64(first - 48)
    i += 1
    while i < start + line_len:
        var c = Int(ptr.unsafe_offset(i)[])
        if not (48 <= c <= 57):
            status = ST_PROTO_ERR
            return 0
        if value > (18446744073709551615 // 10):
            status = ST_PROTO_ERR
            return 0
        value *= 10
        if value > (18446744073709551615 - UInt64(c - 48)):
            status = ST_PROTO_ERR
            return 0
        value += UInt64(c - 48)
        i += 1
    out_end = crlf + 2
    status = ST_OK
    if neg:
        if value > 9223372036854775808:
            status = ST_PROTO_ERR
            return 0
        if value == 9223372036854775808:
            return -9223372036854775807 - 1  # Int64.min
        return -Int(value)
    else:
        if value > 9223372036854775807:
            status = ST_PROTO_ERR
            return 0
        return Int(value)


def _proto_node(pos: Int, msg: String) raises -> Node:
    return Node(ST_PROTO_ERR, pos, PythonObject(from_owned=_bytes_payload(msg)), msg)


def _incomplete_node(pos: Int) raises -> Node:
    return Node(ST_INCOMPLETE, pos, PythonObject(from_owned=_bytes_payload(String())))


# hiredis caps nesting at 1024 containers and reports this exact message
comptime MAX_DEPTH = 1024
# hiredis rejects container counts above UINT32_MAX (measured 3.4.2)
comptime MAX_CONTAINER_ELEMENTS = 4294967295


struct ScanResult(ImplicitlyCopyable):
    """Scan outcome: like Node but without any Python objects."""
    var status: UInt8
    var pos: Int
    var err_msg: String

    def __init__(out self, status: UInt8, pos: Int):
        self.status = status
        self.pos = pos
        self.err_msg = String()

    def __init__(out self, status: UInt8, pos: Int, err_msg: String):
        self.status = status
        self.pos = pos
        self.err_msg = err_msg


def _scan_err[HIGHWAY: Bool](start: Int, msg: String) -> ScanResult:
    """Protocol error result: only highway mode carries the exact message."""
    if HIGHWAY:
        return ScanResult(ST_PROTO_ERR, start, msg)
    else:
        return ScanResult(ST_PROTO_ERR, start)


def _scan_resp[HIGHWAY: Bool](
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int,
    mut slices: List[ResponseSlice], base: Int, depth: Int,
) -> ScanResult:
    """Completeness pass: walks one reply creating no Python objects.

    Classic mode only reports the status; the builder owns the exact error
    text.  HIGHWAY additionally records every byte payload as
    (offset, length, resp_type) relative to `base` and reports protocol
    errors to the consumer directly.  A chunked reply that is still
    incomplete sets Reader.needs_scan so later chunks can resume scanning.
    """
    if start >= end:
        return ScanResult(ST_INCOMPLETE, start)
    var t = ptr.unsafe_offset(start)[]
    if t == TYPE_SIMPLE or t == TYPE_ERROR or t == TYPE_INT or t == 44:
        var crlf = _find_crlf(ptr, start + 1, end)
        if crlf < 0:
            return ScanResult(ST_INCOMPLETE, start)
        if HIGHWAY:
            _record_slice(slices, start + 1 - base, crlf - (start + 1), t)
            return ScanResult(ST_OK, crlf + 2)
        else:
            if t == TYPE_INT:
                var after_crlf = crlf
                var status: UInt8 = ST_OK
                _ = _parse_int_line(ptr, start + 1, crlf, after_crlf, status)
                if status == ST_INCOMPLETE:
                    return ScanResult(ST_INCOMPLETE, start)
                if status == ST_PROTO_ERR:
                    return ScanResult(ST_PROTO_ERR, start)
                return ScanResult(ST_OK, after_crlf)
            return ScanResult(ST_OK, crlf + 2)
    if t == TYPE_BULK:
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var blen = _read_len_fast(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return ScanResult(ST_INCOMPLETE, start)
        if status == ST_PROTO_ERR:
            return _scan_err[HIGHWAY](start, String("Bad bulk string length"))
        if blen < -1:
            return _scan_err[HIGHWAY](start, String("Bulk string length out of range"))
        if blen == -1:
            if HIGHWAY:
                _record_slice(slices, start - base, -1, TYPE_BULK)  # nil row
            return ScanResult(ST_OK, after_int)
        var pstart = after_int
        if blen > end - pstart - 2:
            return ScanResult(ST_INCOMPLETE, start)
        if HIGHWAY:
            _record_slice(slices, pstart - base, blen, TYPE_BULK)
        return ScanResult(ST_OK, pstart + blen + 2)
    if t == 40:  # '(' big number
        var crlf = _find_crlf(ptr, start + 1, end)
        if crlf < 0:
            return ScanResult(ST_INCOMPLETE, start)
        if not _bignum_ok(ptr, start + 1, crlf):
            return _scan_err[HIGHWAY](start, String("Bad bignum value"))
        if HIGHWAY:
            _record_slice(slices, start + 1 - base, crlf - (start + 1), t)
        return ScanResult(ST_OK, crlf + 2)
    if t == TYPE_ARRAY or t == 62 or t == 124:  # '*' / '>' push / '|' attribute
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var count = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return ScanResult(ST_INCOMPLETE, start)
        if status == ST_PROTO_ERR:
            return _scan_err[HIGHWAY](start, String("Bad multi-bulk length"))
        if count > MAX_CONTAINER_ELEMENTS:
            return _scan_err[HIGHWAY](start, String("Multi-bulk length out of range"))
        if t == 124:
            if count < 0:
                return _scan_err[HIGHWAY](start, String("Bad attribute length"))
            count *= 2
        elif count < -1:
            return _scan_err[HIGHWAY](start, String("Multi-bulk length out of range"))
        elif count == -1:
            if HIGHWAY:
                _record_slice(slices, start - base, -1, t)  # nil container row
            return ScanResult(ST_OK, after_int)
        if depth > MAX_DEPTH:
            return _scan_err[HIGHWAY](start, String("Max nesting depth exceeded"))
        if count > (end - after_int) // 3:
            return ScanResult(ST_INCOMPLETE, start)
        if HIGHWAY:
            _record_slice(slices, start - base, count, t)  # container header
        var pos = after_int
        for _i in range(count):
            var child = _scan_resp[HIGHWAY](ptr, pos, end, slices, base, depth + 1)
            if child.status == ST_PROTO_ERR:
                return _scan_err[HIGHWAY](start, child.err_msg)
            if child.status == ST_INCOMPLETE:
                return ScanResult(ST_INCOMPLETE, start)
            pos = child.pos
        return ScanResult(ST_OK, pos)
    if t == 126:  # '~' set
        var after_int2 = start + 1
        var status2: UInt8 = ST_OK
        var count2 = _read_int(ptr, start + 1, end, after_int2, status2)
        if status2 == ST_INCOMPLETE:
            return ScanResult(ST_INCOMPLETE, start)
        if status2 == ST_PROTO_ERR or count2 < 0:
            return _scan_err[HIGHWAY](start, String("Bad set length"))
        if count2 > MAX_CONTAINER_ELEMENTS:
            return _scan_err[HIGHWAY](start, String("Multi-bulk length out of range"))
        if depth > MAX_DEPTH:
            return _scan_err[HIGHWAY](start, String("Max nesting depth exceeded"))
        if count2 > (end - after_int2) // 3:
            return ScanResult(ST_INCOMPLETE, start)
        if HIGHWAY:
            _record_slice(slices, start - base, count2, 126)  # set header
        var spos = after_int2
        for _i2 in range(count2):
            var child2 = _scan_resp[HIGHWAY](ptr, spos, end, slices, base, depth + 1)
            if child2.status == ST_PROTO_ERR:
                return _scan_err[HIGHWAY](start, child2.err_msg)
            if child2.status == ST_INCOMPLETE:
                return ScanResult(ST_INCOMPLETE, start)
            spos = child2.pos
        return ScanResult(ST_OK, spos)
    if t == 37:  # '%' map
        var after_int3 = start + 1
        var status3: UInt8 = ST_OK
        var pairs = _read_int(ptr, start + 1, end, after_int3, status3)
        if status3 == ST_INCOMPLETE:
            return ScanResult(ST_INCOMPLETE, start)
        if status3 == ST_PROTO_ERR or pairs < 0:
            return _scan_err[HIGHWAY](start, String("Bad map length"))
        if pairs > MAX_CONTAINER_ELEMENTS:
            return _scan_err[HIGHWAY](start, String("Multi-bulk length out of range"))
        if depth > MAX_DEPTH:
            return _scan_err[HIGHWAY](start, String("Max nesting depth exceeded"))
        if pairs > (end - after_int3) // 6:
            return ScanResult(ST_INCOMPLETE, start)
        if HIGHWAY:
            _record_slice(slices, start - base, pairs * 2, 37)  # map header
        var mpos = after_int3
        for _i3 in range(pairs):
            var key_node = _scan_resp[HIGHWAY](ptr, mpos, end, slices, base, depth + 1)
            if key_node.status == ST_PROTO_ERR:
                return _scan_err[HIGHWAY](start, key_node.err_msg)
            if key_node.status == ST_INCOMPLETE:
                return ScanResult(ST_INCOMPLETE, start)
            mpos = key_node.pos
            var val_node = _scan_resp[HIGHWAY](ptr, mpos, end, slices, base, depth + 1)
            if val_node.status == ST_PROTO_ERR:
                return _scan_err[HIGHWAY](start, val_node.err_msg)
            if val_node.status == ST_INCOMPLETE:
                return ScanResult(ST_INCOMPLETE, start)
            mpos = val_node.pos
        return ScanResult(ST_OK, mpos)
    if t == 61:  # '=' verbatim: skip the "txt:" prefix like the classic path
        var after_int4 = start + 1
        var status4: UInt8 = ST_OK
        var vlen = _read_int(ptr, start + 1, end, after_int4, status4)
        if status4 == ST_INCOMPLETE:
            return ScanResult(ST_INCOMPLETE, start)
        if status4 == ST_PROTO_ERR or vlen < 0:
            return _scan_err[HIGHWAY](start, String("Bad verbatim string length"))
        var pstart4 = after_int4
        if vlen > end - pstart4 - 2:
            return ScanResult(ST_INCOMPLETE, start)
        if HIGHWAY:
            var colon = _find_byte(ptr, pstart4, pstart4 + vlen, 58)
            var vpos = pstart4 + vlen
            var vlen2 = vlen
            if colon >= 0:
                vpos = colon + 1
                vlen2 = pstart4 + vlen - vpos
            _record_slice(slices, vpos - base, vlen2, t)
        return ScanResult(ST_OK, pstart4 + vlen + 2)
    if t == 35:  # '#' bool
        if start + 2 > end:
            return ScanResult(ST_INCOMPLETE, start)
        var bval = ptr.unsafe_offset(start + 1)[]
        if not _bool_ok(Int(bval)):
            return _scan_err[HIGHWAY](start, String("Bad bool value"))
        if start + 4 > end:
            return ScanResult(ST_INCOMPLETE, start)
        if ptr.unsafe_offset(start + 2)[] != 13 or ptr.unsafe_offset(start + 3)[] != 10:
            return _scan_err[HIGHWAY](start, String("Bad bool value"))
        if HIGHWAY:
            _record_slice(slices, start + 1 - base, 1, 35)  # 't'/'f' byte
        return ScanResult(ST_OK, start + 4)
    if t == 95:  # '_' null
        if start + 3 > end:
            return ScanResult(ST_INCOMPLETE, start)
        if ptr.unsafe_offset(start + 1)[] != 13 or ptr.unsafe_offset(start + 2)[] != 10:
            return _scan_err[HIGHWAY](start, String("Protocol error: invalid null reply"))
        if HIGHWAY:
            _record_slice(slices, start - base, -1, 95)  # null row
        return ScanResult(ST_OK, start + 3)
    if HIGHWAY:
        var msg = String("Protocol error, got ")
        msg += _hex_byte(Int(t))
        msg += " as reply type byte"
        return ScanResult(ST_PROTO_ERR, start, msg)
    else:
        # unknown type byte: let the builder produce the exact error text
        return ScanResult(ST_PROTO_ERR, start)


def _scan_node(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int, depth: Int) -> ScanResult:
    """Classic completeness scan (no slice recording, no messages)."""
    var none_slices = List[ResponseSlice]()
    return _scan_resp[False](ptr, start, end, none_slices, 0, depth)


def _scan_resume(mut r: Reader, ptr: Pointer[UInt8, MutAnyOrigin]) -> UInt8:
    """Resumable completeness scan for a top-level array/push reply.

    Progress commits at child boundaries, so a chunked feed rescans only the
    one child spanning the chunk boundary instead of the whole reply; without
    this the chunked rewrite is O(n^2) in the reply size (measured: 4 KiB
    chunks over a 20k-element array were 50x slower than hiredis).
    """
    if r.scan_active and r.scan_pos < r.consumed:
        r.scan_active = False
    if not r.scan_active:
        var after_int = r.consumed + 1
        var status: UInt8 = ST_OK
        var count = _read_int(ptr, r.consumed + 1, r.buf_len, after_int, status)
        if status == ST_INCOMPLETE:
            return ST_INCOMPLETE
        if status == ST_PROTO_ERR or count < -1:
            return ST_PROTO_ERR
        if count == -1:
            return ST_OK
        r.scan_pos = after_int
        r.scan_remaining = count
        r.scan_active = True
    var none_slices = List[ResponseSlice]()
    while r.scan_remaining > 0:
        var child = _scan_resp[False](ptr, r.scan_pos, r.buf_len, none_slices, 0, 2)
        if child.status == ST_PROTO_ERR:
            r.scan_active = False
            return ST_PROTO_ERR
        if child.status == ST_INCOMPLETE:
            return ST_INCOMPLETE
        r.scan_pos = child.pos
        r.scan_remaining -= 1
    r.scan_active = False
    return ST_OK


def _scan_needs(mut r: Reader, ptr: Pointer[UInt8, MutAnyOrigin]) -> UInt8:
    """Scan the pending reply when a previous build came back incomplete."""
    if r.consumed >= r.buf_len:
        return ST_INCOMPLETE
    var t = ptr.unsafe_offset(r.consumed)[]
    var status: UInt8 = ST_OK
    if t == TYPE_ARRAY or t == 62:
        status = _scan_resume(r, ptr)
    else:
        var sc = _scan_node(ptr, r.consumed, r.buf_len, 1)
        status = sc.status
    if status != ST_INCOMPLETE:
        r.scan_active = False
        r.scan_pos = 0
        r.scan_remaining = 0
    return status


def _parse_node(
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int, depth: Int, mut cnv: DecodeCtx
) raises -> Node:
    if start >= end:
        return _incomplete_node(start)
    var t = ptr.unsafe_offset(start)[]
    if t == TYPE_SIMPLE or t == TYPE_ERROR or t == TYPE_INT:
        var crlf = _find_crlf(ptr, start + 1, end)
        if crlf < 0:
            return _incomplete_node(start)
        var pstart = start + 1
        var plen = crlf - pstart
        if t == TYPE_INT:
            var after_crlf = crlf
            var status: UInt8 = ST_OK
            var value = _parse_int_line(ptr, pstart, crlf, after_crlf, status)
            if status == ST_INCOMPLETE:
                return _incomplete_node(start)
            if status == ST_PROTO_ERR:
                return _proto_node(start, "Bad integer value")
            ref cpy = Python().cpython()
            return Node(ST_OK, after_crlf, PythonObject(from_owned=cpy.PyLong_FromSsize_t(value)))
        if t == TYPE_ERROR:
            var err_node = Node(ST_REPLY_ERR, crlf + 2, PythonObject(from_owned=_error_marker(ptr.unsafe_offset(pstart), plen)))
            err_node.had_err = True
            return err_node^
        var payload = _leaf_payload(ptr, pstart, plen, cnv)
        return Node(ST_OK, crlf + 2, PythonObject(from_owned=payload))
    if t == TYPE_BULK:
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var blen = _read_len_fast(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status == ST_PROTO_ERR:
            return _proto_node(start, "Bad bulk string length")
        if blen < -1:
            return _proto_node(start, "Bulk string length out of range")
        if blen == -1:
            return Node(ST_OK, after_int, PythonObject(from_owned=_none_payload()))
        var pstart = after_int
        if blen > end - pstart - 2:  # no overflow for blen near Int.MAX
            return _incomplete_node(start)
        # hiredis does not validate the trailing CRLF after a bulk payload;
        # it consumes payload + 2 bytes unconditionally (verified 3.4.2)
        return Node(ST_OK, pstart + blen + 2, PythonObject(from_owned=_leaf_payload(ptr, pstart, blen, cnv)))
    if t == 40:  # '(' big number: digits until CRLF, returned as bytes
        var crlf = _find_crlf(ptr, start + 1, end)
        if crlf < 0:
            return _incomplete_node(start)
        if not _bignum_ok(ptr, start + 1, crlf):
            return _proto_node(start, "Bad bignum value")
        return Node(ST_OK, crlf + 2, PythonObject(from_owned=_leaf_payload(ptr, start + 1, crlf - (start + 1), cnv)))
    if t == TYPE_ARRAY or t == 62 or t == 124:  # '*' / '>' push / '|' attribute
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var count = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status == ST_PROTO_ERR:
            return _proto_node(start, "Bad multi-bulk length")
        if count > MAX_CONTAINER_ELEMENTS:
            return _proto_node(start, "Multi-bulk length out of range")
        if t == 124:
            # attributes flatten N key/value pairs into one list (hiredis-py)
            if count < 0:
                return _proto_node(start, "Bad attribute length")
            count *= 2
        elif count < -1:
            return _proto_node(start, "Multi-bulk length out of range")
        elif count == -1:
            return Node(ST_OK, after_int, PythonObject(from_owned=_none_payload()))
        if depth > MAX_DEPTH:
            return _proto_node(start, "Max nesting depth exceeded")
        if count > (end - after_int) // 3:
            # fewer than 3 bytes per element left: this reply can never complete
            return _incomplete_node(start)
        ref cpy = Python().cpython()
        var list_obj = cpy.PyList_New(count)
        var pos = after_int
        var saw_err = False
        for i in range(count):
            # inline scalar fast path: arrays of strings/ints are the common
            # real reply (MGET/LRANGE/HGETALL/SMEMBERS).  A recursive
            # _parse_node call plus its Node costs ~20ns per element
            # (measured), and most elements are one of these three types.
            if pos >= end:
                # the previous element consumed exactly to the chunk end
                _ = cpy.Py_DecRef(list_obj)
                return _incomplete_node(start)
            var t2 = ptr.unsafe_offset(pos)[]
            if t2 == TYPE_BULK:
                var after2 = pos + 1
                var st2: UInt8 = ST_OK
                var blen2 = _read_len_fast(ptr, pos + 1, end, after2, st2)
                if st2 == ST_INCOMPLETE:
                    _ = cpy.Py_DecRef(list_obj)
                    return _incomplete_node(start)
                if st2 == ST_PROTO_ERR:
                    _ = cpy.Py_DecRef(list_obj)
                    return _proto_node(start, "Bad bulk string length")
                if blen2 < -1:
                    _ = cpy.Py_DecRef(list_obj)
                    return _proto_node(start, "Bulk string length out of range")
                if blen2 == -1:
                    _ = cpy.PyList_SetItem(list_obj, i, _none_payload())
                    pos = after2
                else:
                    if after2 + blen2 + 2 > end:
                        _ = cpy.Py_DecRef(list_obj)
                        return _incomplete_node(start)
                    # decoding is per-Reader, so hoist the check out of the
                    # helper: the common no-encoding path skips its frame
                    if cnv.enabled:
                        _ = cpy.PyList_SetItem(list_obj, i, _leaf_payload(ptr, after2, blen2, cnv))
                    else:
                        _ = cpy.PyList_SetItem(list_obj, i, _bytes_slice_payload(ptr, after2, blen2))
                    pos = after2 + blen2 + 2
                continue
            if t2 == TYPE_INT:
                var after2 = pos + 1
                var st2: UInt8 = ST_OK
                var value2 = _read_int(ptr, pos + 1, end, after2, st2)
                if st2 == ST_INCOMPLETE:
                    _ = cpy.Py_DecRef(list_obj)
                    return _incomplete_node(start)
                if st2 == ST_PROTO_ERR:
                    _ = cpy.Py_DecRef(list_obj)
                    return _proto_node(start, "Bad integer value")
                _ = cpy.PyList_SetItem(list_obj, i, cpy.PyLong_FromSsize_t(value2))
                pos = after2
                continue
            if t2 == TYPE_SIMPLE or t2 == TYPE_ERROR:
                var crlf2 = _find_crlf(ptr, pos + 1, end)
                if crlf2 < 0:
                    _ = cpy.Py_DecRef(list_obj)
                    return _incomplete_node(start)
                var pstart2 = pos + 1
                if t2 == TYPE_ERROR:
                    _ = cpy.PyList_SetItem(
                        list_obj, i,
                        _error_marker(ptr.unsafe_offset(pstart2), crlf2 - pstart2))
                    saw_err = True
                elif cnv.enabled:
                    _ = cpy.PyList_SetItem(
                        list_obj, i,
                        _leaf_payload(ptr, pstart2, crlf2 - pstart2, cnv))
                else:
                    _ = cpy.PyList_SetItem(
                        list_obj, i,
                        _bytes_slice_payload(ptr, pstart2, crlf2 - pstart2))
                pos = crlf2 + 2
                continue
            var child = _parse_node(ptr, pos, end, depth + 1, cnv)
            if child.status == ST_INCOMPLETE:
                _ = cpy.Py_DecRef(list_obj)
                return _incomplete_node(start)
            if child.status == ST_PROTO_ERR:
                _ = cpy.Py_DecRef(list_obj)
                return _proto_node(start, child.err_msg)
            if child.status == ST_DICT_ERR:
                _ = cpy.Py_DecRef(list_obj)
                return child^
            if child.had_err:
                saw_err = True
            if child.status == ST_PUSH:
                # nested push: tag for the wrapper's PushNotification walk
                saw_err = True
                _ = cpy.PyList_SetItem(list_obj, i, _push_marker(child.payload.steal_data()))
            else:
                _ = cpy.PyList_SetItem(list_obj, i, child.payload.steal_data())
            pos = child.pos
        var out_status: UInt8 = ST_PUSH if t == 62 else ST_OK
        var out_node = Node(out_status, pos, PythonObject(from_owned=list_obj))
        out_node.had_err = saw_err
        return out_node^
    if t == 44:  # ',' double
        var crlf = _find_crlf(ptr, start + 1, end)
        if crlf < 0:
            return _incomplete_node(start)
        var pstart = start + 1
        if crlf - pstart >= 326:  # hiredis char buf[326]
            return _proto_node(start, "Double value is too large")
        if not _double_chars_ok(ptr, pstart, crlf):
            return _proto_node(start, "Bad double value")
        # borrow the CRLF byte as strtod's NUL terminator, then restore it
        ptr.unsafe_offset(crlf)[] = 0
        var errno_ptr = external_call["__errno_location", Pointer[c_int, MutAnyOrigin]]()
        errno_ptr[] = 0
        var endp = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=Int(ptr) + crlf)
        var dval = external_call["strtod", Float64](ptr.unsafe_offset(pstart), Pointer(to=endp))
        var err = errno_ptr[]  # ERANGE == 34: overflow and underflow both fail
        ptr.unsafe_offset(crlf)[] = 13
        if Int(endp) != Int(ptr) + crlf or err == 34:
            return _proto_node(start, "Bad double value")
        return Node(ST_OK, crlf + 2, PythonObject(from_owned=Python().cpython().PyFloat_FromDouble(dval)))
    if t == 35:  # '#' bool: #t\r\n / #f\r\n
        if start + 2 > end:
            return _incomplete_node(start)
        var bval = ptr.unsafe_offset(start + 1)[]
        if not _bool_ok(Int(bval)):
            return _proto_node(start, "Bad bool value")
        if start + 4 > end:
            return _incomplete_node(start)
        if ptr.unsafe_offset(start + 2)[] != 13 or ptr.unsafe_offset(start + 3)[] != 10:
            return _proto_node(start, "Bad bool value")
        ref cpy2 = Python().cpython()
        if bval == 116 or bval == 84:
            return Node(ST_OK, start + 4, PythonObject(from_owned=cpy2.PyBool_FromLong(1)))
        return Node(ST_OK, start + 4, PythonObject(from_owned=cpy2.PyBool_FromLong(0)))
    if t == 95:  # '_' null
        if start + 3 > end:
            return _incomplete_node(start)
        if ptr.unsafe_offset(start + 1)[] != 13 or ptr.unsafe_offset(start + 2)[] != 10:
            return _proto_node(start, "Protocol error: invalid null reply")
        return Node(ST_OK, start + 3, PythonObject(from_owned=_none_payload()))
    if t == 61:  # '=' verbatim string
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var vlen = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status == ST_PROTO_ERR or vlen < 0:
            return _proto_node(start, "Bad verbatim string length")
        var pstart = after_int
        if vlen > end - pstart - 2:
            return _incomplete_node(start)
        var colon = _find_byte(ptr, pstart, pstart + vlen, 58)
        var vpos = pstart + vlen
        var vlen2 = vlen
        if colon >= 0:
            vpos = colon + 1
            vlen2 = pstart + vlen - vpos
        return Node(ST_OK, pstart + vlen + 2, PythonObject(from_owned=_leaf_payload(ptr, vpos, vlen2, cnv)))
    if t == 126:  # '~' set -> plain list (hiredis-py parity)
        var after_int2 = start + 1
        var status2: UInt8 = ST_OK
        var count2 = _read_int(ptr, start + 1, end, after_int2, status2)
        if status2 == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status2 == ST_PROTO_ERR or count2 < 0:
            return _proto_node(start, "Bad set length")
        if count2 > MAX_CONTAINER_ELEMENTS:
            return _proto_node(start, "Multi-bulk length out of range")
        if depth > MAX_DEPTH:
            return _proto_node(start, "Max nesting depth exceeded")
        if count2 > (end - after_int2) // 3:
            return _incomplete_node(start)
        ref cpy3 = Python().cpython()
        var set_list = cpy3.PyList_New(count2)
        var spos = after_int2
        var saw_err2 = False
        for i2 in range(count2):
            var child2 = _parse_node(ptr, spos, end, depth + 1, cnv)
            if child2.status == ST_INCOMPLETE:
                _ = cpy3.Py_DecRef(set_list)
                return _incomplete_node(start)
            if child2.status == ST_PROTO_ERR:
                _ = cpy3.Py_DecRef(set_list)
                return _proto_node(start, child2.err_msg)
            if child2.status == ST_DICT_ERR:
                _ = cpy3.Py_DecRef(set_list)
                return child2^
            if child2.had_err:
                saw_err2 = True
            if child2.status == ST_PUSH:
                saw_err2 = True
                _ = cpy3.PyList_SetItem(set_list, i2, _push_marker(child2.payload.steal_data()))
            else:
                _ = cpy3.PyList_SetItem(set_list, i2, child2.payload.steal_data())
            spos = child2.pos
        var set_node = Node(ST_OK, spos, PythonObject(from_owned=set_list))
        set_node.had_err = saw_err2
        return set_node^
    if t == 37:  # '%' map -> dict (N key/value pairs)
        var after_int3 = start + 1
        var status3: UInt8 = ST_OK
        var pairs = _read_int(ptr, start + 1, end, after_int3, status3)
        if status3 == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status3 == ST_PROTO_ERR or pairs < 0:
            return _proto_node(start, "Bad map length")
        if pairs > MAX_CONTAINER_ELEMENTS:
            return _proto_node(start, "Multi-bulk length out of range")
        if depth > MAX_DEPTH:
            return _proto_node(start, "Max nesting depth exceeded")
        if pairs > (end - after_int3) // 6:
            return _incomplete_node(start)
        ref cpy4 = Python().cpython()
        var dict_obj = cpy4.PyDict_New()
        var mpos = after_int3
        var saw_err3 = False
        for i3 in range(pairs):
            var key_node = _parse_node(ptr, mpos, end, depth + 1, cnv)
            if key_node.status == ST_INCOMPLETE:
                _ = cpy4.Py_DecRef(dict_obj)
                return _incomplete_node(start)
            if key_node.status == ST_PROTO_ERR:
                _ = cpy4.Py_DecRef(dict_obj)
                return _proto_node(start, key_node.err_msg)
            if key_node.status == ST_DICT_ERR:
                _ = cpy4.Py_DecRef(dict_obj)
                return key_node^
            if key_node.had_err:
                saw_err3 = True
            mpos = key_node.pos
            var val_node = _parse_node(ptr, mpos, end, depth + 1, cnv)
            if val_node.status == ST_INCOMPLETE:
                _ = cpy4.Py_DecRef(dict_obj)
                return _incomplete_node(start)
            if val_node.status == ST_PROTO_ERR:
                _ = cpy4.Py_DecRef(dict_obj)
                return _proto_node(start, val_node.err_msg)
            if val_node.status == ST_DICT_ERR:
                _ = cpy4.Py_DecRef(dict_obj)
                return val_node^
            if val_node.had_err:
                saw_err3 = True
            mpos = val_node.pos
            # PyDict_SetItem increfs, so release the refs we stole either way
            var kp = key_node.payload.steal_data()
            var vp = val_node.payload.steal_data()
            if val_node.status == ST_PUSH:
                saw_err3 = True
                vp = _push_marker(vp)
            var rc = cpy4.PyDict_SetItem(dict_obj, kp, vp)
            _ = cpy4.Py_DecRef(kp)
            _ = cpy4.Py_DecRef(vp)
            if rc != 0:
                # unhashable key: keep CPython's TypeError (type, args and
                # message) for the wrapper to raise
                var dict_exc = external_call["PyErr_GetRaisedException", PyObjectPtr]()
                _ = cpy4.Py_DecRef(dict_obj)
                if Int(dict_exc) == 0:
                    return Node(ST_DICT_ERR, mpos, PythonObject(from_owned=_none_payload()))
                return Node(ST_DICT_ERR, mpos, PythonObject(from_owned=dict_exc))
        var map_node = Node(ST_OK, mpos, PythonObject(from_owned=dict_obj))
        map_node.had_err = saw_err3
        return map_node^
    var msg = String("Protocol error, got ")
    msg += _hex_byte(Int(t))
    msg += " as reply type byte"
    return _proto_node(start, msg)


def _find_byte(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int, byte: UInt8) -> Int:
    var hit = external_call["memchr", Pointer[UInt8, MutAnyOrigin]](
        ptr.unsafe_offset(start), c_int(Int(byte)), c_size_t(end - start))
    if Int(hit) == 0:
        return -1
    return Int(hit) - Int(ptr)



def _hex_byte(b: Int) -> String:
    """Replicates hiredis C chrtos() quoting for reply-type bytes."""
    comptime HEXDIGITS = "0123456789abcdef"
    if b == 92:  # backslash
        return String("\"\\\\\"")
    if b == 34:  # double quote
        return String("\"\\\"\"")
    if b == 10:
        return String("\"\\n\"")
    if b == 13:
        return String("\"\\r\"")
    if b == 9:
        return String("\"\\t\"")
    if b == 7:
        return String("\"\\a\"")
    if b == 8:
        return String("\"\\b\"")
    if 32 <= b <= 126:
        var printed = String("\"")
        printed += chr(b)
        printed += "\""
        return printed
    var out = String("\"\\x")
    out += HEXDIGITS[byte=(b >> 4) & 15]
    out += HEXDIGITS[byte=b & 15]
    out += "\""
    return out


# === Highway scan (leaf slices only) ===

def _scan_highway(
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int,
    mut slices: List[ResponseSlice], base: Int, depth: Int,
) -> ScanResult:
    """Highway completeness scan: records payload slices and error messages."""
    return _scan_resp[True](ptr, start, end, slices, base, depth)


def _record_slice(mut slices: List[ResponseSlice], offset: Int, length: Int, resp_type: UInt8):
    var s = ResponseSlice()
    s.offset = offset
    s.length = length
    s.resp_type = resp_type
    slices.append(s^)


# === Module entry ===

@export
def PyInit_hpredis_core() abi("C") -> PythonObject:
    try:
        var m = PythonModuleBuilder("hpredis_core")
        _ = m.add_type[Reader]("Reader") \
            .def_init_defaultable[Reader]() \
            .def_method[Reader.feed]("feed") \
            .def_method[Reader.try_gets]("try_gets") \
            .def_method[Reader.prefetch_clean]("prefetch_clean") \
            .def_method[Reader.free]("free") \
            .def_method[Reader.dispose]("dispose") \
            .def_method[Reader.set_decoding]("set_decoding") \
            .def_method[Reader.set_reply_error]("set_reply_error") \
            .def_method[Reader.set_maxbuf]("set_maxbuf") \
            .def_method[Reader.clear_decoding]("clear_decoding") \
            .def_method[Reader.drain]("drain") \
            .def_method[Reader.buffered]("buffered") \
            .def_method[Reader.highway_gets]("highway_gets")
        return m.finalize()
    except e:
        abort(String("error creating hpredis_core:", e))
