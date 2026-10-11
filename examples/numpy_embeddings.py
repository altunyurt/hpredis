"""MGET-style embeddings into numpy.

Run: python examples/numpy_embeddings.py

Highway mode returns the reply as (table, arena): the payload bytes stay in
one buffer, so numpy can view them without building a Python object per row.
"""

import numpy as np
import hpredis

rows, dim = 1000, 1536
vec = np.arange(dim, dtype=np.float32)
wire = b"*%d\r\n" % rows + (b"$%d\r\n" % vec.nbytes + vec.tobytes() + b"\r\n") * rows

reader = hpredis.Reader(highway_mode=True)
reader.feed(wire)
table, arena = reader.gets()

# 1) Zero-copy: a strided view over the arena.  Rows are not contiguous
#    because the wire interleaves framing ($6144\r\n ... \r\n) between them.
t = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
view = np.ndarray((rows, dim), np.float32, buffer=arena,
                  offset=int(t[1][0]), strides=(int(t[2][0]) - int(t[1][0]), 4))
assert view.shape == (rows, dim) and np.array_equal(view[0], vec)

# 2) Contiguous copy in one native call (all payloads the same length).
flat = hpredis.highway_gather(table, arena)
contig = np.frombuffer(flat, dtype=np.float32).reshape(rows, dim)

# 3) Or into a buffer you own, e.g. pinned memory for an async GPU copy.
out = np.empty((rows, dim), np.float32)
hpredis.highway_gather(table, arena, -1, dim * 4, out)

print("numpy embeddings OK:", view.shape, contig.shape, out.shape)
