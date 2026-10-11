# Examples

Each script is runnable from the repo root and asserts its own result, so
`pytest` keeps them honest:

| script | what it shows | needs |
|---|---|---|
| `drop_in_hiredis.py` | `Reader` swap, and redis-py parsing through hpredis | fakeredis |
| `numpy_embeddings.py` | MGET embeddings: strided zero-copy view, `highway_gather`, preallocated `out` | numpy |
| `arrow_dataframes.py` | fixed-width payloads into pandas/polars; variable blobs into zero-copy Arrow `binary_view` | pyarrow, pandas, polars |
| `vector_search.py` | FT.SEARCH KNN reply into vectors ready for a model | numpy |

```bash
uv sync                          # dev group has all of the above
python examples/numpy_embeddings.py
```
