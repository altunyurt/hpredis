**The wrapper is almost certainly your main bottleneck, ahead of anything in the Mojo parser.** You build every reply in Mojo and then rebuild it in Python.

## The hot path

```python
status, payload = self._core.try_gets()   # Mojo call + tuple alloc
...                                       # 5 failed `status ==` tests on the common path
return self._finalize(payload, should_decode)
```

`_finalize` is the killer. For a clean reply with no encoding and no errors, it does nothing useful, but it still does the following:

- Runs `isinstance(obj, tuple)`, then `isinstance(obj, list)`.
- Recurses with a list comprehension, calling `_finalize` once per element, each with 2-3 `isinstance` calls.
- Throws away the list Mojo built and allocates a second, identical one.

For a 100-element array that's roughly 100 Python calls at about 150-250 ns each, which is 15-25 µs. hiredis parses the whole thing in a few µs. These are my estimates, not measurements. For a scalar reply like `+OK`, the stack is `gets()` → `try_gets()` → tuple unpack → 5 comparisons → `_finalize()` → 2 `isinstance`, versus one C call in hiredis. You paid for a Mojo core and then spent the savings in the interpreter.

Your `drain()` already avoids this with `had_markers`, but `gets()` doesn't. `try_gets` throws `node.had_err` away.

## Other wrapper costs

- **`feed(buf, 0, n)`:** redis-py calls it this way, which takes the slow path. It runs `len()`, several comparisons and `memoryview(data)[a:b]`, which creates two memoryview objects, before the core call. hiredis does offset and length natively. Do the offset arithmetic in Mojo from the `Py_buffer` you already have (`view.buf + start`).
- **Status order:** the successful reply, `status == 0`, is tested last.
- **Decoding in Python:** with `decode_responses=True`, every bulk string goes through `_finalize` and then `obj.decode(self._encoding, self._errors or "strict")`. That's two Python calls per element, plus a bytes object that's created and immediately discarded. Mojo can call `PyUnicode_DecodeUTF8` or `PyUnicode_Decode` at the leaf and create a `str` directly. hiredis does exactly this. This is probably the biggest gap if your benchmark uses decode_responses.
- **`pack_command` in Python:** it uses `isinstance` chains, `%` formatting, `+` concatenation and `b"".join`, which makes many temporaries. If redis-py is using hiredis's `pack_command` (I believe it does when available), you're slower than C on every command sent. This is a place Mojo can win: do one pass to compute sizes, call `PyBytes_FromStringAndSize(NULL, total)` once, and write directly into it.

## Correctness gaps in `_finalize` and `gets`

- **Dicts are never walked.** `_finalize` handles tuple, list and bytes only. RESP3 map replies are not decoded when an encoding is set, and error markers inside map values stay as raw `(sentinel, msg)` tuples.
- **Push notifications are not finalized.** `PushNotification(payload)` skips both decoding and marker conversion.
- **`drain()` has the same marker and dict gaps.** It also loses the push distinction.

Moving decoding and error construction into Mojo fixes all of these at once.

## Fastest fix, a minimal patch

In Mojo, return a distinct status when a reply is clean and when it contains nested error markers:

```mojo
comptime ST_OK_MARKERS = 6
# in try_gets:
if node.status == ST_OK or node.status == ST_PUSH:
    ...
    var st = ST_OK_MARKERS if node.had_err else node.status
    return _status_tuple(st, node.payload.steal_data())
```

In Python:

```python
def gets(self, should_decode=True):
    status, payload = self._core.try_gets()
    if status == 0:
        if self._encoding is None or not should_decode:
            return payload                    # common case: zero Python work
        return self._finalize(payload, True)
    if status == 6:
        return self._finalize(payload, should_decode)
    if status == 1:
        return self._notEnoughData
    ...
```

Expect a big drop from this alone for non-decoding use.

## Real fix: delete the wrapper from the hot path

1. **Move `gets` into Mojo.** Pass `replyError`, `notEnoughData`, the encoding and the errors mode into the core constructor. Take `should_decode` as an argument. The leaf builder then creates `PyUnicode_Decode(...)` or bytes straight from the buffer. Error replies call `PyObject_CallOneArg(replyError, msg)`.
2. **Handle replyError exceptions carefully.** redis-py's `parse_error` raises. The call returns NULL with the Python error already set. You must decref the partial list (`Py_DECREF` tolerates NULL slots) and propagate. Check whether the builder preserves an already-set `PyErr` when you `raise`. If it replaces it with a generic Exception, return a sentinel and let a thin shim re-raise, or register the method with a raw C-API signature.
3. **Alias the core methods onto the instance** so there is no Python frame on the hot path:
   ```python
   self._core = _core.Reader(...)
   self.gets = self._core.gets
   self.feed = self._core.feed
   ```
   This needs optional arguments on the Mojo side (`gets(False)` and `feed(data, off, len)`). Use the builder's variadic registration (the `def_py_function` style) and parse the args tuple yourself. That's still cheaper than a Python wrapper.
4. **Decode in Mojo**, including maps and pushes, and delete `_finalize` entirely.

The earlier fixes in the Mojo review still apply: the leaks on incomplete or partial replies, and the two-phase scan-then-build design. Do the wrapper work first, though. It's the bigger win, and it will make the remaining parser costs visible.

## Confirm it

Run this to see how much is wrapper versus core:

```python
import timeit, hiredis, hpredis
payload = b"*100\r\n" + b"$10\r\n0123456789\r\n" * 100

def run(r_feed, r_get):
    r_feed(payload); r_get()

h = hiredis.Reader(); p = hpredis.Reader()
print("hiredis      ", timeit.timeit(lambda: run(h.feed, h.gets), number=50000))
print("hpredis gets ", timeit.timeit(lambda: run(p.feed, p.gets), number=50000))
print("core only    ", timeit.timeit(lambda: run(p._core.feed, p._core.try_gets), number=50000))
```

I expect the third line to be roughly level with or faster than hiredis, and the second to be several times slower. If so, that gap is pure wrapper.

Please also send your benchmark script, or tell me whether you test through redis-py with `decode_responses=True` or at Reader level. That decides whether decode-in-Mojo or `pack_command` matters more for you.
