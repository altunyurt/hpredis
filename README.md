# hpredis

A RESP protocol reader for Python, written in Mojo.

`hpredis.Reader` implements the same interface as `hiredis.Reader`, so
existing hiredis-py code can switch by changing the import. The reader also
has a highway mode that returns raw byte offsets into its receive buffer,
which NumPy and native code can read without copying.

## Aim

The project aims to be a RESP reader that existing hiredis-py and redis-py
users can adopt without rewriting code: the same constructor, reply types,
error text, and edge cases. On top of that, highway mode hands large payloads
to native consumers as byte offsets, so a vector can be read by NumPy or an
extension without building a Python object per payload. The remaining work is
listed in the Status section.

```python
import numpy as np
import hpredis

# Classic mode: the hiredis.Reader interface.
r = hpredis.Reader()
r.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
assert r.gets() == [b"hello", b"world"]

# Highway mode: one call returns an offsets table plus the buffer.
vec = np.arange(1536, dtype=np.float32).tobytes()
h = hpredis.Reader(highway_mode=True)
h.feed(b"$%d\r\n" % len(vec) + vec + b"\r\n")
table, arena = h.gets()
triples = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
off, length, resp_type = triples[0]
arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)

# Bulk decoders: one native pass over the table, no per-row Python work.
# highway_gather copies equal-length payloads back to back into a contiguous
# array (or into `out`, e.g. pinned memory).  first_row=-1 auto-skips the
# reply's container header and keeps a scalar reply's row.
blob = hpredis.highway_gather(table, arena)   # same bytes as vec

# highway_ints/highway_floats parse decimal payloads (SMEMBERS, ZRANGE, ...).
i = hpredis.Reader(highway_mode=True)
i.feed(b"*3\r\n$1\r\n7\r\n$3\r\n-12\r\n$2\r\n42\r\n")
it, ia = i.gets()
nums = np.frombuffer(hpredis.highway_ints(it, ia), dtype="<i8")   # [7, -12, 42]

# Zero-copy Arrow: the arena stays the BinaryView data buffer (needs pyarrow).
from hpredis.arrow import binary_array
col = binary_array(table, arena)      # pa.binary_view(), values point into arena
```

## Reading RediSearch replies (FT.SEARCH)

A KNN reply is `[count, id, [field, value, ...], id, [...], ...]`, so the
rows are heterogeneous: the table is the data.  Read it as a numpy array of
`(offset, length, type)` triples and select the payloads you need instead of
building a Python list per field:

```python
t, arena = reader.gets()
rows = np.frombuffer(t, dtype="<i8").reshape(-1, 3)
# payload rows are type '$' with length >= 0; filter to the 512-dim vectors
payloads = rows[(rows[:, 2] == ord("$")) & (rows[:, 1] == 2048)]
vecs = [np.frombuffer(arena, dtype=np.float32, offset=int(off), count=512)
        for off, _ in payloads[:, :2]]
```

Container header rows carry their child count in the length column (`*`/`>`/
`~`/`%`/`|`), so which document a field belongs to is recoverable from the
same array; scalar nils have length -1.  The parse itself is microseconds on
typical top-K replies, so reach for highway mode when the reply is large, not
for latency on a 10-hit search.

## Feature comparison

| | hiredis-py 3.4.2 | hpredis |
|---|---|---|
| `Reader` API (`feed`, `gets`, `len`, `has_data`, `set_encoding`, `setmaxbuf`, `getmaxbuf`, `close`) | yes | yes |
| `pack_command`, `HiredisError`, `ProtocolError`, `ReplyError`, `PushNotification` | yes | yes |
| RESP2, and the RESP3 types redis-py uses (maps, sets, pushes, doubles, booleans, nulls, big numbers, verbatim strings, attributes) | yes | yes |
| Error text, length caps, and nesting depth | reference | matching, checked by the tests and a differential fuzzer |
| Batched parse through `drain()` | no | yes, with an optional `max_replies` cap |
| Zero-copy payload reads through highway mode | no | yes, byte offsets into the receive buffer |
| Bulk decoders over a highway table (`highway_ints`, `highway_floats`, `highway_gather`) | no | yes, one native pass |
| Zero-copy Arrow `binary_view` import (`hpredis.arrow`, optional pyarrow) | no | yes, the arena is the data buffer |
| redis-py 8.1 (sync client, pipelines, `decode_responses`) | yes | yes, tested through fakeredis |
| Runtime dependencies | none | none |
| Implementation | C extension, vendored hiredis | Mojo extension |
| Python versions | 3.9 to 3.15, PyPy | 3.12 and newer |
| Platforms | Linux, macOS, Windows, FreeBSD | Linux wheel; source build needs the Mojo toolchain |

## Using it as a drop-in

Programs that import hiredis-py can switch by changing the import:

```python
import hpredis as hiredis
```

For redis-py, route the reader before the first connection is created:

```python
import sys
import hpredis

sys.modules["hiredis"] = hpredis
import redis
```

If redis-py is already imported, replace the attributes it reads:

```python
import hiredis
import hpredis

hiredis.Reader = hpredis.Reader
hiredis.PushNotification = hpredis.PushNotification
```

`hpredis.Reader` accepts the constructor arguments redis-py passes,
including `protocolError`, `replyError`, `encoding`, `errors`, and
`notEnoughData`. `hpredis.__version__` reports the hiredis-py release whose
behavior it matches (3.4.2), which is the value redis-py checks before it
enables its hiredis parser.

## Highway mode

`Reader(highway_mode=True)` returns `(table, arena)` from `gets()`. The table
holds 24-byte rows of little-endian int64 values `(offset, length, resp_type)`
with offsets into the arena, a `bytearray` that holds the reply. A view of
the arena keeps it alive, so payloads stay valid after the reader is
collected or new data is fed. The rows cover every item in the reply in wire
order: byte payloads carry a slice, nil has `length = -1`, booleans carry
their `t` or `f` byte, and a container has a header row whose length is its
child count.

## Install

Python 3.12 or newer. A prebuilt wheel needs no toolchain:

```bash
pip install hpredis-0.1.0-cp312-cp312-linux_x86_64.whl
```

Installing from a source checkout requires the Mojo toolchain
(`pip install mojo`), which `pip install .` invokes through `build.sh`:

```bash
pip install .
```

## Development

```bash
uv sync
./build.sh
.venv/bin/python -m pytest -q
uv build --wheel
```

## Tests

The suite covers the upstream hiredis-py Reader tests, a seeded differential
fuzzer that compares hpredis against hiredis-py, memory and robustness
regressions (leaks, hostile length headers, nesting depth, chunked feeds),
highway mode, and redis-py integration through fakeredis.

## Status

Classic mode parses RESP2 and RESP3 and matches hiredis-py on the tested
behavior and error cases. `feed()` and `gets()` are single native calls:
every core method is registered through CPython's vectorcall slot instead of
the Mojo binding trampoline, and `gets()` returns the finished object from
the core, so a reply never re-enters Python. `drain()` still leads on large
batches. The gap that remains is inside the core parser (Python object
construction, nested containers), not the Mojo boundary. Planned work:
Python 3.9 to 3.11 support, published wheels for more platforms, and a
native `Reader` type to drop the last Python frame.

## Limitations

- `setmaxbuf()` stores the hiredis-compatible value and uses it as the idle
  buffer trim threshold. It does not reject large replies, matching hiredis.
- Protocol error messages match hiredis byte for byte, including escapes.
- Highway mode returns byte offsets for native consumers; classic mode is the
  path for Python reply objects.
