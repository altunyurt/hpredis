# Phase 4: AI Highway — Mojo core (phase 3 core + memoryview + compaction-safe highway).
# Compile: mojo build phase3.mojo --emit shared-lib -o hpredis_core.so

from std.python import Python, PythonObject
from std.python.python_object import PyObjectPtr
from std.python.bindings import PythonModuleBuilder
from std.os import abort
from std.ffi import external_call, c_int, c_ssize_t, c_size_t
from std.collections import Array, List
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

# nested error marker sentinel (tuple[0]); unique, never valid RESP data
comptime ERR_SENTINEL = "\x00hpredis-error\x00"


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

    def __init__(out self, status: UInt8, pos: Int, payload: PythonObject):
        self.status = status
        self.pos = pos
        self.payload = payload
        self.err_msg = String()

    def __init__(out self, status: UInt8, pos: Int, payload: PythonObject, err_msg: String):
        self.status = status
        self.pos = pos
        self.payload = payload
        self.err_msg = err_msg


struct Reader(Defaultable, Movable, Writable):
    var buf_addr: Int
    var buf_cap: Int
    var buf_len: Int
    var consumed: Int
    var proto_err: Bool
    var proto_err_msg: String
    var highway_slices: List[ResponseSlice]
    var highway_base: Int

    def __init__(out self):
        self.buf_addr = 0
        self.buf_cap = 0
        self.buf_len = 0
        self.consumed = 0
        self.proto_err = False
        self.proto_err_msg = String()
        self.highway_slices = List[ResponseSlice]()
        self.highway_base = 0

    # === Python-facing methods ===

    @staticmethod
    def feed(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], data: PythonObject) raises -> PythonObject:
        """Copy a buffer-protocol object (bytes/bytearray/memoryview) into the arena."""
        var raw = data.steal_data()
        var view = PyBuffer()
        var rc = external_call["PyObject_GetBuffer", c_int](
            raw, Pointer(to=view), c_int(0))  # PyBUF_SIMPLE
        if rc != 0:
            raise Error("feed() expects a buffer-protocol object")
        var n = Int(view.len)
        if n > 0:
            _ensure_cap(self_ptr[], self_ptr[].buf_len + n)
            var dst = Pointer[UInt8, MutAnyOrigin](
                unsafe_from_address=self_ptr[].buf_addr + self_ptr[].buf_len)
            _ = external_call["memcpy", Pointer[UInt8, MutAnyOrigin]](
                dst, view.buf.value(), c_size_t(n))
            self_ptr[].buf_len += n
        _ = external_call["PyBuffer_Release", NoneType](Pointer(to=view))
        return PythonObject(0)

    @staticmethod
    def try_gets(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        if self_ptr[].proto_err:
            # sticky protocol error (matches hiredis): keep raising
            return _status_tuple(ST_PROTO_ERR, _bytes_payload(self_ptr[].proto_err_msg))
        if self_ptr[].consumed >= self_ptr[].buf_len:
            return _status_tuple(ST_INCOMPLETE, _none_payload())
        var ptr = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_addr)
        var node = _parse_node(ptr, self_ptr[].consumed, self_ptr[].buf_len)
        if node.status == ST_INCOMPLETE:
            return _status_tuple(ST_INCOMPLETE, _none_payload())
        if node.status == ST_OK:
            self_ptr[].consumed = node.pos
            _ = _compact(self_ptr[])
            return _status_tuple(ST_OK, node.payload.steal_data())
        if node.status == ST_REPLY_ERR:
            # consumed (matches hiredis); wrapper builds the replyError instance
            self_ptr[].consumed = node.pos
            _ = _compact(self_ptr[])
            return _status_tuple(ST_REPLY_ERR, node.payload.steal_data())
        # protocol error: sticky, not consumed
        self_ptr[].proto_err = True
        self_ptr[].proto_err_msg = node.err_msg
        return _status_tuple(ST_PROTO_ERR, node.payload.steal_data())

    @staticmethod
    def buffered(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        return PythonObject(self_ptr[].buf_len - self_ptr[].consumed)

    @staticmethod
    def highway_gets(self_ptr: Pointer[mut=True, Self, MutAnyOrigin]) raises -> PythonObject:
        if self_ptr[].consumed >= self_ptr[].buf_len:
            return _tuple3(ST_INCOMPLETE, 0, 0)
        var ptr = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self_ptr[].buf_addr)
        var start = self_ptr[].consumed
        self_ptr[].highway_slices.clear()
        var pos = _scan_highway(ptr, start, self_ptr[].buf_len, self_ptr[].highway_slices, start)
        if pos < 0:
            return _tuple3(ST_INCOMPLETE, 0, 0)
        self_ptr[].consumed = pos
        # No compaction on the highway path: memmove would overwrite the
        # reply we just exposed via memoryviews/pointers.
        self_ptr[].highway_base = self_ptr[].buf_addr + start
        return _tuple3(ST_OK, self_ptr[].highway_base, len(self_ptr[].highway_slices))

    @staticmethod
    def highway_slice(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], i: PythonObject) raises -> PythonObject:
        var idx = Int(py=i)
        if idx < 0 or idx >= len(self_ptr[].highway_slices):
            raise Error("slice index out of range")
        var s = self_ptr[].highway_slices[idx]
        ref cpy = Python().cpython()
        var t = cpy.PyTuple_New(3)
        _ = cpy.PyTuple_SetItem(t, 0, cpy.PyLong_FromSsize_t(self_ptr[].highway_base + s.offset))
        _ = cpy.PyTuple_SetItem(t, 1, cpy.PyLong_FromSsize_t(s.length))
        _ = cpy.PyTuple_SetItem(t, 2, cpy.PyLong_FromSsize_t(Int(s.resp_type)))
        return PythonObject(from_owned=t)

    @staticmethod
    def memoryview(self_ptr: Pointer[mut=True, Self, MutAnyOrigin], i: PythonObject) raises -> PythonObject:
        """Zero-copy read-only memoryview over highway slice i (PyBUF_READ)."""
        var idx = Int(py=i)
        if idx < 0 or idx >= len(self_ptr[].highway_slices):
            raise Error("slice index out of range")
        var s = self_ptr[].highway_slices[idx]
        if s.length <= 0:
            raise Error("slice has no data")
        var obj = external_call["PyMemoryView_FromMemory", PyObjectPtr](
            Pointer[UInt8, MutAnyOrigin](
                unsafe_from_address=self_ptr[].highway_base + s.offset),
            c_ssize_t(s.length),
            c_int(0x100))  # PyBUF_READ
        return PythonObject(from_owned=obj)

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

def _ensure_cap(mut r: Reader, needed: Int) raises:
    if needed <= r.buf_cap:
        return
    var new_cap = r.buf_cap if r.buf_cap > 0 else 4096
    while new_cap < needed:
        new_cap *= 2
    var newp = external_call["realloc", Pointer[UInt8, MutAnyOrigin]](
        Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_addr), c_size_t(new_cap))
    if Int(newp) == 0:
        raise Error("out of memory")
    r.buf_addr = Int(newp)
    r.buf_cap = new_cap


def _compact(mut r: Reader) -> Bool:
    """Returns True if a memmove compaction happened."""
    if r.consumed == 0:
        return False
    if r.consumed == r.buf_len:
        r.buf_len = 0
        r.consumed = 0
        return False
    if r.consumed * 2 >= r.buf_len and r.buf_len > 1024:
        var dst = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_addr)
        var src = Pointer[UInt8, MutAnyOrigin](unsafe_from_address=r.buf_addr + r.consumed)
        _ = external_call["memmove", Pointer[UInt8, MutAnyOrigin]](
            dst, src, c_size_t(r.buf_len - r.consumed))
        r.buf_len -= r.consumed
        r.consumed = 0
        return True
    return False


# === Result helpers ===

def _none_payload() -> PyObjectPtr:
    return Python().cpython().Py_None()  # borrowed; None is immortal


def _bytes_payload(s: String) raises -> PyObjectPtr:
    # String utf8 bytes via PyBytes
    return external_call["PyBytes_FromStringAndSize", PyObjectPtr](
        s.unsafe_ptr(), c_ssize_t(s.byte_length()))


def _bytes_slice_payload(ptr: Pointer[UInt8, MutAnyOrigin], offset: Int, length: Int) raises -> PyObjectPtr:
    return external_call["PyBytes_FromStringAndSize", PyObjectPtr](
        ptr.unsafe_offset(offset), c_ssize_t(length))


def _status_tuple(status: UInt8, payload: PyObjectPtr) raises -> PythonObject:
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(2)
    _ = cpy.PyTuple_SetItem(t, 0, cpy.PyLong_FromSsize_t(Int(status)))
    _ = cpy.PyTuple_SetItem(t, 1, payload)
    return PythonObject(from_owned=t)


def _tuple3(a: UInt8, b: Int, c: Int) raises -> PythonObject:
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(3)
    _ = cpy.PyTuple_SetItem(t, 0, cpy.PyLong_FromSsize_t(Int(a)))
    _ = cpy.PyTuple_SetItem(t, 1, cpy.PyLong_FromSsize_t(b))
    _ = cpy.PyTuple_SetItem(t, 2, cpy.PyLong_FromSsize_t(c))
    return PythonObject(from_owned=t)


def _error_marker(msg_ptr: Pointer[UInt8, MutAnyOrigin], length: Int) raises -> PyObjectPtr:
    """Nested error reply → (sentinel_bytes, message_bytes)."""
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(2)
    var sentinel = _bytes_payload(String(ERR_SENTINEL))
    _ = cpy.PyTuple_SetItem(t, 0, sentinel)
    _ = cpy.PyTuple_SetItem(t, 1, _bytes_slice_payload(msg_ptr, 0, length))
    return t


# === RESP parsing ===

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
    """Parse signed int + CRLF, replicating hiredis C readLine + string2ll
    exactly: find the CRLF first (no CRLF -> incomplete), then validate the
    whole line strictly (leading zeros rejected, unsigned 64-bit overflow
    math). status: 0 ok / 1 incomplete / 2 protocol error."""
    var crlf = _find_crlf(ptr, start, end)
    if crlf < 0:
        status = ST_INCOMPLETE
        return 0
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


def _parse_node(ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int) raises -> Node:
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
            var value = _read_int(ptr, pstart, crlf + 2, after_crlf, status)
            if status == ST_INCOMPLETE:
                return _incomplete_node(start)
            if status == ST_PROTO_ERR:
                return _proto_node(start, "Bad integer value")
            ref cpy = Python().cpython()
            return Node(ST_OK, after_crlf, PythonObject(from_owned=cpy.PyLong_FromSsize_t(value)))
        var payload = _bytes_slice_payload(ptr, pstart, plen)
        if t == TYPE_ERROR:
            return Node(ST_REPLY_ERR, crlf + 2, PythonObject(from_owned=_error_marker(ptr.unsafe_offset(pstart), plen)))
        return Node(ST_OK, crlf + 2, PythonObject(from_owned=payload))
    if t == TYPE_BULK:
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var blen = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status == ST_PROTO_ERR:
            return _proto_node(start, "Bad bulk string length")
        if blen < -1:
            return _proto_node(start, "Bulk string length out of range")
        if blen == -1:
            return Node(ST_OK, after_int, PythonObject(from_owned=_none_payload()))
        var pstart = after_int
        if pstart + blen + 2 > end:
            return _incomplete_node(start)
        # hiredis does not validate the trailing CRLF after a bulk payload;
        # it consumes payload + 2 bytes unconditionally (verified 3.4.2)
        return Node(ST_OK, pstart + blen + 2, PythonObject(from_owned=_bytes_slice_payload(ptr, pstart, blen)))
    if t == TYPE_ARRAY:
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var count = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return _incomplete_node(start)
        if status == ST_PROTO_ERR:
            return _proto_node(start, "Bad multi-bulk length")
        if count < -1:
            return _proto_node(start, "Multi-bulk length out of range")
        if count == -1:
            return Node(ST_OK, after_int, PythonObject(from_owned=_none_payload()))
        ref cpy = Python().cpython()
        var list_obj = cpy.PyList_New(count)
        var pos = after_int
        for i in range(count):
            var child = _parse_node(ptr, pos, end)
            if child.status == ST_INCOMPLETE:
                return _incomplete_node(start)
            if child.status == ST_PROTO_ERR:
                return _proto_node(start, child.err_msg)
            _ = cpy.PyList_SetItem(list_obj, i, child.payload.steal_data())
            pos = child.pos
        return Node(ST_OK, pos, PythonObject(from_owned=list_obj))
    var msg = String("Protocol error, got ")
    msg += _hex_byte(Int(t))
    msg += " as reply type byte"
    return _proto_node(start, msg)


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


def _payload_str(payload: PythonObject) raises -> String:
    return String(payload)


# === Highway scan (leaf slices only) ===

def _scan_highway(
    ptr: Pointer[UInt8, MutAnyOrigin], start: Int, end: Int, mut slices: List[ResponseSlice], base: Int
) -> Int:
    """Scan one reply; returns end pos, or -1 if incomplete. Proto errors: -2.
    Slice offsets are recorded relative to `base` (the reply start)."""
    if start >= end:
        return -1
    var t = ptr.unsafe_offset(start)[]
    if t == TYPE_SIMPLE or t == TYPE_ERROR or t == TYPE_INT:
        var crlf = _find_crlf(ptr, start + 1, end)
        if crlf < 0:
            return -1
        _record_slice(slices, start + 1 - base, crlf - (start + 1), t)
        return crlf + 2
    if t == TYPE_BULK:
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var blen = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return -1
        if status == ST_PROTO_ERR or blen < -1:
            return -2
        if blen == -1:
            return after_int
        var pstart = after_int
        if pstart + blen + 2 > end:
            return -1
        _record_slice(slices, pstart - base, blen, TYPE_BULK)
        return pstart + blen + 2
    if t == TYPE_ARRAY:
        var after_int = start + 1
        var status: UInt8 = ST_OK
        var count = _read_int(ptr, start + 1, end, after_int, status)
        if status == ST_INCOMPLETE:
            return -1
        if status == ST_PROTO_ERR or count < -1:
            return -2
        if count == -1:
            return after_int
        var pos = after_int
        for _ in range(count):
            pos = _scan_highway(ptr, pos, end, slices, base)
            if pos < 0:
                return -1
        return pos
    return -2


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
            .def_method[Reader.buffered]("buffered") \
            .def_method[Reader.highway_gets]("highway_gets") \
            .def_method[Reader.highway_slice]("highway_slice") \
            .def_method[Reader.memoryview]("memoryview")
        return m.finalize()
    except e:
        abort(String("error creating hpredis_core:", e))
