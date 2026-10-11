# Benchmarks

- `bench.py` — the local matrix (hpredis vs hiredis): corpora, chunked
  reads, consumers. Every run prints its own legend (unit, statistic,
  scope). `python src/benchmark/bench.py`.
- `bench_simd.mojo` — CRLF scan strategies (memchr vs SIMD vs scalar):
  `mojo build src/benchmark/bench_simd.mojo -o /tmp/bench_simd && /tmp/bench_simd`.
- `with_hpredis.py` — run redis-py's own benchmarks from a redis-py checkout
  with hpredis in place of the hiredis module:

```bash
cd /path/to/redis-py
PYTHONPATH=/path/to/hpredis/src python /path/to/hpredis/src/benchmark/with_hpredis.py \
    -m benchmarks.basic_operations -n 100000 -P 1 -s 2
```

`HP_PACK=0` keeps hiredis's `pack_command` so only the parser differs.
Sequential small commands measure the server round-trip, not the reader;
use `-P` (pipeline) or `-s` (payload size) to put the parser on the hot
path. Script benchmarks work the same way
(`... with_hpredis.py benchmarks/connection_handshake.py`).

Restart Redis before a comparison and interleave the configurations instead
of running them back to back: a server that has been up under load drifts,
which once turned a 7% difference into 11% between two runs of the same
workload.
