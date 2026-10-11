"""Zero-copy Arrow import of a highway table.

pyarrow is an optional dependency: importing this module imports pyarrow.
Long payloads (> 12 bytes) stay in the arena and the views point into it;
values of 12 bytes or less are inlined in the 16-byte view, which is the
Arrow BinaryView layout, not a copy we add.
"""

import pyarrow as pa

from . import _highway_arrow_views


def binary_array(table, arena, first_row=-1):
    """pa.Array of binary_view whose values point into `arena`.

    `table` and `arena` are the pair from Reader(highway_mode=True).gets().
    The array holds both buffers alive, so the arena stays valid; feeding the
    Reader again detaches a pinned arena instead of reusing it.  Scalar nil
    rows become Arrow nulls; container rows raise ValueError.  first_row=-1
    auto-skips the reply's container header row.
    """
    views, validity = _highway_arrow_views(table, arena, first_row)
    n = len(views) // 16
    if n == 0:
        return pa.array([], type=pa.binary_view())
    return pa.Array.from_buffers(
        pa.binary_view(), n,
        [pa.py_buffer(validity), pa.py_buffer(views), pa.py_buffer(arena)],
    )
