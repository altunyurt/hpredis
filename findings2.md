Here's what's left from my first review. I'm working from the code I read earlier, so tell me if you've already touched any of these. I'd do the leaks first, since they're cheap and can bite in production, then the performance items in order of expected payoff.

## 1. Leaks and crashes (fix first)

- **Error replies:** the `payload` bytes created in the simple/error branch of `_parse_node` is never used or freed on the `TYPE_ERROR` path. Also cache the sentinel bytes object instead of rebuilding it from a `String` on every error.
- **Partial containers:** on a child INCOMPLETE or PROTO_ERR, `list_obj`, `set_list` and `dict_obj` are dropped without a decref. This happens on every chunked large reply.
- **Maps:** `PyDict_SetItem` doesn't steal references, so `steal_data()` on the key and value leaks both. This hits every RESP3 map.
- **`feed()`:** `data.steal_data()` probably leaks a reference per chunk. Verify with `sys.getrefcount(b)` before and after `feed(b)`.
- **None refcount:** this is only safe on Python ≥ 3.12. On 3.9-3.11 it corrupts the refcount, so incref it or require 3.12.
- **Hostile input:** cap `count` against the remaining bytes (reject `count > (end - pos) / 3` as incomplete) before `PyList_New`, and cap the nesting depth (hiredis caps it too).

The two-phase design below fixes most of the first three structurally, but the one-line fixes are worth doing now regardless.

## 2. Performance, by expected payoff

1. **Two-phase parse (scan, then build).** This removes the restart-from-scratch behavior on partial reads. Today a large reply arriving in N chunks rebuilds all its objects N times. It also deletes the `Node` struct and all the status plumbing. Benchmark 100k × 10-byte bulks fed in 16 KB chunks. I expect the biggest remaining gap there.
2. **Per-node overhead.** Drop `Node` (with its `PythonObject` and `String`) for out-params or direct list-slot writes. Hoist `Python().cpython()` out of the hot path.
3. **Header parsing.** Replace the libc `memchr` on 2-4 byte ranges with an inline one-pass digit loop, falling back to your strict `_read_int` for leading zeros or long numbers. Also stop scanning ints twice (`_find_crlf`, then `_read_int` scans again).
4. **Native decoding.** If `decode_responses=True` still goes through Python, call `PyUnicode_DecodeUTF8` (or `PyUnicode_Decode`) in Mojo at the leaf. This is still worth checking in your current wrapper, and it also fixes the dict and push gaps.
5. **Remove the `try_gets` status tuple.** Return the object directly, with the incomplete and error cases signalled differently. Alias core methods onto the instance so there's no Python frame on `gets`.
6. **`feed(buf, off, len)` natively.** This avoids the double memoryview slice on redis-py's path. Longer term, hand out a writable arena tail for `sock.recv_into` plus a `commit(n)`, which gives true zero-copy from the socket.
7. **`pack_command` in Mojo.** One size pass, one `PyBytes_FromStringAndSize(NULL, total)`, then write directly.
8. **Small wins:** hoist `consumed` and `buf_len` into locals in `drain` (the `MutAnyOrigin` aliasing issue), use `PyFloat_FromString` for doubles instead of the round trip through `Float64`, add singleton bytes for `+OK`, `+QUEUED` and `+PONG`, and check the build is `-O3`.

## 3. Highway mode (only if you're keeping it)

- `_scan_highway` returns -2 for errors and RESP3 types, but `highway_gets` treats every negative value as INCOMPLETE, so callers hang.
- The memoryviews have no owner. A later `feed()` realloc or a `drain` memmove leaves them dangling.
- The arena never resets or compacts in highway mode, so it grows unbounded.
- Per-leaf `highway_slice` calls cost more than just creating the bytes. If you keep it, return one offsets/lengths table instead.

## 4. Cleanup

- Delete the unused, inexact `_parse_float`.
- Update the stale compile comment (`phase3.mojo`).

If you send the updated code and the numbers from the benchmark in my last message, I can tell you which item dominates what's left. My guess is #2 (two-phase parse) for large replies and #4 for decoded workloads.
