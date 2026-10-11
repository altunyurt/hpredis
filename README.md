# hpredis

A drop-in replacement for hiredis-py, written in Mojo, with a zero-copy
highway mode for bulk payloads.

## Drop-in

```python
import hpredis as hiredis

r = hiredis.Reader()
r.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
assert r.gets() == [b"hello", b"world"]
```

redis-py, before the first connection:

```python
import sys
import hpredis

sys.modules["hiredis"] = hpredis
import redis
```

redis-py already imported:

```python
import hiredis
import hpredis

hiredis.Reader = hpredis.Reader
hiredis.PushNotification = hpredis.PushNotification
```

`hpredis.__version__` is `3.4.2`, the hiredis-py release whose behavior it
matches — the value redis-py checks before enabling its hiredis parser.

## Highway mode

`Reader(highway_mode=True)` returns `(table, arena)`: 24-byte `(offset,
length, resp_type)` rows into a `bytearray` holding the reply. No Python
object per payload.

```python
import numpy as np
import hpredis

vec = np.arange(1536, dtype=np.float32).tobytes()
r = hpredis.Reader(highway_mode=True)
r.feed(b"$%d\r\n" % len(vec) + vec + b"\r\n")
table, arena = r.gets()
off, length, _ = np.frombuffer(table, dtype="<i8").reshape(-1, 3)[0]
arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
```

Rows cover every item in wire order: payload slice, nil `-1`, bool its `t`/`f`
byte, container a header row with its child count. A view keeps the arena
alive, so payloads stay valid after the reader is collected or new data fed.

### Bulk decoders

One native pass over the table:

```python
flat = hpredis.highway_gather(table, arena)        # equal-length payloads, contiguous
out = np.empty_like(flat)
hpredis.highway_gather(table, arena, -1, 1536 * 4, out)   # or into a buffer you own

i = hpredis.Reader(highway_mode=True)              # decimal payloads -> int64/float64
i.feed(b"*3\r\n$1\r\n7\r\n$3\r\n-12\r\n$2\r\n42\r\n")
it, ia = i.gets()
ints = np.frombuffer(hpredis.highway_ints(it, ia), dtype="<i8")   # [7, -12, 42]
```

### Arrow

```python
from hpredis.arrow import binary_array

col = binary_array(table, arena)   # pa.binary_view() over the arena, no copy
```

### FT.SEARCH

A KNN reply is `[count, id, [field, value, ...], ...]`: heterogeneous rows, so
select from the table instead of building a list per field.

```python
# table, arena from highway_gets() on the KNN reply
rows = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
vecs = [np.frombuffer(arena, np.float32, offset=int(off), count=512)
        for off, _ in rows[(rows[:, 2] == ord("$")) & (rows[:, 1] == 2048)][:, :2]]
```

## Examples

[`examples/`](examples/): drop-in, numpy embeddings, Arrow/pandas/polars,
FT.SEARCH. Each asserts its own result and runs in the test suite.

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
| redis-py 8.1 (sync + asyncio clients, pipelines, `decode_responses`) | yes | yes; the hiredis parser path is driven directly in tests, plus fakeredis integration |
| Runtime dependencies | none | none |
| Implementation | C extension, vendored hiredis | Mojo extension |
| Python versions | 3.9 to 3.15, PyPy | 3.12 and newer |
| Platforms | Linux, macOS, Windows, FreeBSD | Linux wheel; source build needs the Mojo toolchain |

## Install

Python 3.12 or newer.

```bash
pip install hpredis-0.1.0-cp312-cp312-linux_x86_64.whl   # prebuilt wheel, no toolchain
pip install .                                            # source build needs Mojo
```

## Development

```bash
uv sync
./build.sh
.venv/bin/python -m pytest -q
uv build --wheel
```

## Tests

Upstream hiredis-py Reader tests, a seeded differential fuzzer against
hiredis-py, memory and robustness regressions, highway mode, redis-py's sync
and asyncio hiredis parser paths, and fakeredis integration.

## Status

Classic mode matches hiredis-py on RESP2/RESP3 behavior and errors; `feed()`
and `gets()` are single native calls. The remaining gap is inside the core
parser (object construction, nested containers), not the Mojo boundary.
Planned: Python 3.9 to 3.11, more wheels, a native `Reader` type.

## Limitations

- `setmaxbuf()` is the hiredis idle-trim threshold; it does not reject large
  replies (hiredis parity).
- Protocol error messages match hiredis byte for byte.
- Highway mode is for native consumers; classic mode returns Python objects.
