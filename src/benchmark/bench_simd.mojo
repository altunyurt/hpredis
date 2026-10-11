# Benchmark: CRLF scan strategies for the RESP parser.
# Compares (1) glibc memchr (current implementation), (2) hand-rolled
# SIMD filter (Mojo 1.1 SIMD, no packed bitmask) + scalar tail-find,
# (3) plain scalar loop. Run: mojo build src/benchmark/bench_simd.mojo && ./bench_simd

from std.simd import SIMD
from std.builtin.dtype import DType
from std.ffi import external_call, c_int, c_size_t
from std.origin import MutAnyOrigin
from std.collections import Array, List
from std.time import perf_counter_ns as now


def make_corpus(header_bytes: Int, bulk_len: Int, count: Int) -> List[UInt8]:
    """Parser-shaped data: *N header then count bulk payloads of bulk_len
    bytes separated by CRLF."""
    var buf = List[UInt8]()
    # header like: *10000\r\n$64\r\n
    var header = String("*")
    header += String(count)
    header += "\r\n$"
    header += String(bulk_len)
    header += "\r\n"
    for c in header.bytes():

        buf.append(Int(c).cast[DType.uint8]())
    var fill: UInt8 = 65
    for _ in range(count):
        for _ in range(bulk_len):
            buf.append(fill)
            fill = (fill + 1) % 240
        buf.append(13)
        buf.append(10)
    return buf^


def crlf_memchr(ptr: Pointer[UInt8, MutAnyOrigin], length: Int) -> Int:
    """Return byte index of '\r' with '\n' after, or -1 (current engine scan)."""
    var p = ptr
    var remaining = length
    var off = 0
    while remaining > 0:
        var hit = external_call["memchr", Pointer[UInt8, MutAnyOrigin]](
            p, c_int(13), c_size_t(remaining))
        if Int(hit) == 0:
            return -1
        var pos = Int(hit) - Int(ptr)
        if pos + 1 < length and ptr.unsafe_offset(pos + 1)[] == 10:
            return pos
        p = ptr.unsafe_offset(pos + 1)
        remaining = length - (pos + 1)
    return -1


def crlf_scalar(ptr: Pointer[UInt8, MutAnyOrigin], length: Int) -> Int:
    var i = 0
    while i + 1 < length:
        if ptr.unsafe_offset(i)[] == 13 and ptr.unsafe_offset(i + 1)[] == 10:
            return i
        i += 1
    return -1


def crlf_simd(ptr: Pointer[UInt8, MutAnyOrigin], length: Int) -> Int:
    """SIMD filter: any '\r' in a 16-byte chunk? If yes, scalar-find it."""
    comptime W = 16
    var pos = 0
    while pos + W <= length:
        var v = ptr.unsafe_offset(pos).unsafe_bitcast[SIMD[DType.uint8, W]]()[]
        var any_cr = v.eq(SIMD[DType.uint8, W](13)).reduce_or()
        if not any_cr[0]:
            pos += W
            continue
        var end = pos + W
        var i = pos
        while i + 1 < end:
            if ptr.unsafe_offset(i)[] == 13 and ptr.unsafe_offset(i + 1)[] == 10:
                return i
            i += 1
        pos = end
    # tail
    var i = pos
    while i + 1 < length:
        if ptr.unsafe_offset(i)[] == 13 and ptr.unsafe_offset(i + 1)[] == 10:
            return i
        i += 1
    return -1


def bench(name: String, ptr: Pointer[UInt8, MutAnyOrigin], length: Int, scan_fn: String):
    var iters = 200_000
    var found = 0
    var t0 = now()
    if scan_fn == "memchr":
        for i in range(iters):
            var off = (i * 7919) % (length - 64)  # rotate start: defeats folding
            var r = crlf_memchr(ptr.unsafe_offset(off), length - off)
            found += r
            ptr.unsafe_offset((i + found) % 64)[] = UInt8(r)  # result feeds a store
    elif scan_fn == "scalar":
        for i in range(iters):
            var off = (i * 7919) % (length - 64)
            var r = crlf_scalar(ptr.unsafe_offset(off), length - off)
            found += r
            ptr.unsafe_offset((i + found) % 64)[] = UInt8(r)
    else:
        for i in range(iters):
            var off = (i * 7919) % (length - 64)
            var r = crlf_simd(ptr.unsafe_offset(off), length - off)
            found += r
            ptr.unsafe_offset((i + found) % 64)[] = UInt8(r)
    var dt = now() - t0
    var ns_per_scan = dt / iters
    var mb = Float64(length) * Float64(iters) / (Float64(dt) / 1e9) / 1e6
    print(name, "|", scan_fn, "|", ns_per_scan, "ns/scan |", mb, "MB/s | found", found)


def main():
    print("corpus | scan | ns/scan | MB/s")
    # corpus A: header-heavy (CRLF every ~12 bytes) -> many matches
    var a = make_corpus(14, 2, 40_000)
    bench("hdr-40k", a.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(a), "memchr")
    bench("hdr-40k", a.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(a), "scalar")
    bench("hdr-40k", a.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(a), "simd")
    # corpus B: 64-byte bulks (CRLF every ~66 bytes) -> multibulk-10k shape
    var b = make_corpus(14, 64, 10_000)
    bench("bulk64-10k", b.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(b), "memchr")
    bench("bulk64-10k", b.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(b), "scalar")
    bench("bulk64-10k", b.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(b), "simd")
    # corpus C: 1 KB payloads (CRLF rare)
    var c = make_corpus(14, 1024, 4_000)
    bench("bulk1k-4k", c.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(c), "memchr")
    bench("bulk1k-4k", c.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(c), "scalar")
    bench("bulk1k-4k", c.unsafe_ptr().unsafe_origin_cast[MutAnyOrigin](), len(c), "simd")
