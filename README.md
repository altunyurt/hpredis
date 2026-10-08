# hpredis — a Mojo drop-in replacement for hiredis-py

A Mojo-based RESP parser with a hiredis-py-compatible `Reader` API, plus an
optional zero-copy **Highway Mode** for AI/ML vector workloads.

```python
import hpredis
import numpy as np

# classic: drop-in for hiredis.Reader (feed/gets, redis-py compatible)
r = hpredis.Reader()
r.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
assert r.gets() == [b"hello", b"world"]

# highway: one call gives an offsets table plus the buffer it points into
h = hpredis.Reader(highway_mode=True)
vector_bytes = bytes(6144)  # replace with a packed vector
h.feed(b"$6144\r\n" + vector_bytes + b"\r\n")
table, arena = h.gets()                     # n*24 bytes: (offset, length, type)
# read the whole table in one call (don't struct.unpack per slice)
triples = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
off, length, resp_type = triples[0]
arr = np.frombuffer(arena, dtype=np.float32, offset=off, count=length // 4)
```

## Features

- `Reader(protocolError, replyError, encoding, errors, notEnoughData,
  highway_mode)` — hiredis-py 3.4.2 Reader compatibility, checked against the
  upstream Reader tests
- redis-py 8.1 integration tested end-to-end (ping/get/set/hset/incr/lrange/
  pipelines, `decode_responses`, `CLIENT SETINFO`, fragmented recv)
- Highway mode: one call returns an offsets table plus the `bytearray` it
  points into, so NumPy/PyTorch/JAX read payloads without a copy and without a
  per-slice call; views own their bytes (valid across later feeds)

## Install

```bash
pip install hpredis   # platform wheel, no toolchain required
pip install .         # source checkout: builds the Mojo extension
```

A prebuilt wheel ships `hpredis_core.so`, so installing it needs no
toolchain. Installing from a clone compiles `src/hpredis_core.mojo` with
`build.sh` and therefore requires `pip install mojo`.

## Development

```bash
uv sync                      # venv: mojo, pytest, hiredis, fakeredis, numpy
./build.sh                   # compile src/hpredis_core.mojo (MOJO=... overrides)
.venv/bin/python -m pytest -q
uv build --wheel             # platform wheel with the prebuilt .so
```

## Benchmarking

Measure representative reply shapes separately: single-reply `gets()`,
buffered `drain()`, and native Highway consumers have different costs.

## Architecture

```
Python bytes ──feed──► Mojo-managed bytearray arena
                          │  RESP scanner (memchr CRLF search)
                          ├─► classic: CPython C-API objects (list/bytes/int)
                          └─► highway: (offsets table, arena) for native reads
```

## Limitations

- Per-reply `gets()` is slower for tiny responses; `drain()` batches buffered
  replies and is faster for pipeline workloads
- Highway Mode exposes a pre-order table, not nested Python reply objects:
  payload slices, `length = -1` for nil, and a header row (child count) per
  container; it is intended for native consumers
- `setmaxbuf()` stores the hiredis-compatible value but does not enforce it
- Highway views keep their arena alive across feeds; the reader detaches rather
  than overwriting bytes that are still viewed
- Protocol-error messages match hiredis byte-for-byte, including escapes
