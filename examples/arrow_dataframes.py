"""Highway tables into Arrow, pandas and polars.

Run: python examples/arrow_dataframes.py   (needs pyarrow, pandas, polars)

Fixed-width payloads become one numpy array first; variable-length blobs go
through hpredis.arrow.binary_array, which keeps the arena as the Arrow data
buffer, so long payloads are not copied.
"""

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import hpredis
from hpredis.arrow import binary_array

# Fixed width: 200 rows x 8 float32 -> one contiguous array -> dataframes.
dim = 8
vec = np.arange(dim, dtype=np.float32)
wire = b"*200\r\n" + (b"$%d\r\n" % vec.nbytes + vec.tobytes() + b"\r\n") * 200
r = hpredis.Reader(highway_mode=True)
r.feed(wire)
t, arena = r.gets()
m = np.frombuffer(hpredis.highway_gather(t, arena), np.float32).reshape(200, dim)
pdf = pd.DataFrame(m)
pldf = pl.DataFrame(m)
assert pdf.shape == pldf.shape == (200, dim)

# Variable length: blobs (e.g. an LRANGE of JPEGs) -> Arrow binary_view.
blobs = [b"jpeg" * i for i in range(1, 21)]
wire = b"*%d\r\n" % len(blobs) + b"".join(
    b"$%d\r\n%s\r\n" % (len(b), b) for b in blobs)
r = hpredis.Reader(highway_mode=True)
r.feed(wire)
t, arena = r.gets()
arr = binary_array(t, arena)
tbl = pa.table({"blob": arr})
assert tbl.column("blob").to_pylist() == blobs
# pandas has no binary_view dtype, so to_pandas() materializes bytes;
# polars reads the Arrow buffers.
assert len(tbl.to_pandas()) == len(blobs)
assert pl.from_arrow(tbl).height == len(blobs)

print("arrow/dataframes OK:", arr.type, pdf.shape, pldf.shape)
