"""FT.SEARCH KNN reply -> vectors ready for a model.

Run: python examples/vector_search.py

The reply is [count, id, [field, value, ...], id, [...], ...]: heterogeneous
rows, so select the payload rows you need from the table instead of walking
Python objects.  The vectors stay views into the arena; torch.from_numpy()
adds no copy on CPU, and .to("cuda") is the only transfer.
"""

import numpy as np
import hpredis

dim, k = 512, 100
vectors = np.random.default_rng(0).random((k, dim), dtype=np.float32)

wire = b"*%d\r\n" % (2 * k + 1) + b"$%d\r\n%d\r\n" % (len(str(k)), k)
for i, v in enumerate(vectors):
    doc = b"doc:%d" % i
    wire += (b"$%d\r\n%s\r\n" % (len(doc), doc)
             + b"*6\r\n"
             + b"$2\r\nid\r\n" + b"$%d\r\n%s\r\n" % (len(doc), doc)
             + b"$4\r\ndist\r\n" + b"$6\r\n0.0001\r\n"
             + b"$3\r\nvec\r\n" + b"$%d\r\n%s\r\n" % (v.nbytes, v.tobytes()))

reader = hpredis.Reader(highway_mode=True)
reader.feed(wire)
table, arena = reader.gets()

# Payload rows are type '$' with length >= 0; the vectors are dim*4 bytes.
rows = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
vec_rows = rows[(rows[:, 2] == ord("$")) & (rows[:, 1] == dim * 4)]
hits = np.stack([np.frombuffer(arena, np.float32, offset=int(off), count=dim)
                 for off, _ in vec_rows[:, :2]])
assert hits.shape == (k, dim) and np.array_equal(hits[0], vectors[0])

# For a reply of scores only (e.g. ZRANGE WITHSCORES), highway_floats parses
# the whole selection in one native pass:
#     scores = np.frombuffer(hpredis.highway_floats(t, arena), "<f8")
# then: import torch; batch = torch.from_numpy(hits).to("cuda")

print("vector search OK:", hits.shape, hits.dtype)
