"""Benchmark matrix: hpredis (Mojo) vs hiredis-py (C).

Advisory numbers, single machine, no isolation.  Run:
    python src/benchmark/bench.py

Every cell is wall-clock throughput in MB/s = payload bytes x iterations /
seconds, best of 3 timed runs.  Timed per iteration: feed() + gets() + the
consumer work named in the row (view creation, decode, ...).  Excluded:
socket reads, the Redis server, GPU transfers, process start.  The legend
printed at the start names every row and the statistic.
"""

import gc
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]

# build the extension when it is missing or older than its source (the same
# staleness rule conftest uses); scrub the variables the extension's import
# sets at the C level, which children must not inherit
SO = ROOT / "src" / "hpredis" / "hpredis_core.so"
SRC = ROOT / "src" / "hpredis_core.mojo"
if not SO.exists() or SO.stat().st_mtime < SRC.stat().st_mtime:
    ENV = {k: v for k, v in os.environ.items()
           if k not in ("PYTHONPATH", "PYTHONEXECUTABLE", "PYTHONHOME")}
    subprocess.run(["bash", str(ROOT / "build.sh")], check=True,
                   capture_output=True, env=ENV)

sys.path.insert(0, str(ROOT / "src"))

import hiredis  # noqa: E402
import hpredis  # noqa: E402


def make_corpora():
    # 1) big multi-bulk: 10k x 64-byte strings (~660 KB)
    bulk64 = b"$64\r\n" + b"x" * 64 + b"\r\n"
    big_mb = b"*10000\r\n" + bulk64 * 10000
    # 2) pipeline of simple replies
    simples = b"+OK\r\n" * 100_000
    # 3) 1536-dim float32 vector
    vector = b"".join(struct.pack("f", float(i)) for i in range(1536))
    vec_bulk = b"$%d\r\n" % len(vector) + vector + b"\r\n"
    # 4) deep nesting
    deep = b"*1\r\n" * 50 + b":1\r\n"
    return {
        "multibulk-10k": (big_mb, 20),
        "simples-100k": (simples, 5),  # 5 feeds x 100k replies = 500k gets
        "vector-1536f32": (vec_bulk, 20_000),
        "deep-50": (deep, 20_000),
    }


def run_classic(payload, iters, Reader, encoding=None, errors="replace"):
    reader = Reader(encoding=encoding, errors=errors) if encoding else Reader()
    reader.feed(payload)
    t0 = time.perf_counter()
    for _ in range(iters):
        reader.feed(payload)
        while reader.gets() is not False:
            pass
    return time.perf_counter() - t0


def run_highway(payload, iters, materialize=False):
    """Highway gets() returns (table, arena): a table of (offset, length, type)
    triples and the bytearray holding the reply.

    materialize=False times the scan and table build only.  materialize=True
    also reads each slice the way a native consumer does - np.frombuffer on the
    arena, no per-slice bridge call and no Python object per payload - which is
    the like-for-like comparison against classic mode creating bytes objects.
    """
    reader = hpredis.Reader(highway_mode=True)
    reader.feed(payload)
    t0 = time.perf_counter()
    for _ in range(iters):
        reader.feed(payload)
        while True:
            got = reader.gets()
            if got is False:
                break
            if materialize:
                table, arena = got
                # consumer model: one numpy call for the table, then one row
                # view per payload.  No .tolist() on the columns: that builds
                # a Python int per slice, which no consumer does and which
                # dominated the earlier hw+views numbers.
                triples = np.frombuffer(table, dtype="<i8").reshape(-1, 3).tolist()
                for off, length, _ in triples:
                    if length > 0:
                        np.frombuffer(arena, dtype=np.uint8, offset=off, count=length)
    return time.perf_counter() - t0


def run_drain(payload, iters):
    reader = hpredis.Reader()
    reader.feed(payload)
    t0 = time.perf_counter()
    for _ in range(iters):
        reader.feed(payload)
        while reader.drain():
            pass
    return time.perf_counter() - t0


def gc_stats(fn):
    before = gc.get_stats()
    fn()
    after = gc.get_stats()
    pauses = sum(a["collected"] - b["collected"] for a, b in zip(after, before))
    return pauses


def best(fn, reps=3):
    """Best of `reps`; single runs on a shared machine drift by ±25%."""
    return min(fn() for _ in range(reps))


def bench_one(name, payload, iters):
    nbytes = len(payload) * iters
    t_hiredis = best(lambda: run_classic(payload, iters, hiredis.Reader))
    t_hpredis = best(lambda: run_classic(payload, iters, hpredis.Reader))
    t_highway = best(lambda: run_highway(payload, iters)) if b"\r\n" in payload else None
    t_highway_mat = (
        best(lambda: run_highway(payload, iters, materialize=True))
        if b"\r\n" in payload else None
    )
    t_drain = best(lambda: run_drain(payload, iters))
    # decode path: what redis-py's decode_responses=True pays (errors=replace
    # so the binary vector corpus stays decodable)
    t_dec_h = best(lambda: run_classic(payload, iters, hiredis.Reader, encoding="utf-8"))
    t_dec_p = best(lambda: run_classic(payload, iters, hpredis.Reader, encoding="utf-8"))
    print(
        f"  {name} decode | hiredis {nbytes / t_dec_h / 1e6:8.1f} MB/s"
        f" | hpredis {nbytes / t_dec_p / 1e6:8.1f} MB/s | x{t_dec_h / t_dec_p:4.2f}"
    )
    gc_hiredis = gc_stats(lambda: run_classic(payload, iters, hiredis.Reader))
    gc_hpredis = gc_stats(lambda: run_classic(payload, iters, hpredis.Reader))
    # (gc counters are cumulative: one run each, not best-of-N)
    row = [
        name,
        f"hiredis {nbytes / t_hiredis / 1e6:8.1f} MB/s",
        f"hpredis {nbytes / t_hpredis / 1e6:8.1f} MB/s",
        f"x{t_hiredis / t_hpredis:4.2f}",
        f"gc {gc_hiredis:3d}/{gc_hpredis:3d}",
    ]
    if t_highway:
        row.append(f"highway {nbytes / t_highway / 1e6:8.1f} MB/s")
        row.append(f"hw+views {nbytes / t_highway_mat / 1e6:8.1f} MB/s")
    row.append(f"drain {nbytes / t_drain / 1e6:8.1f} MB/s")
    print(" | ".join(row))


def make_ai_corpora():
    """The shapes an ML client actually sends on the wire.

    `mget`: MGET of 1000 keys, each holding a 1536-dim float32 embedding
    (6144 bytes) - one reply, ~6.1 MB of payload.
    `stream`: the same reply arriving as a socket would deliver it, in 64 KiB
    reads, which is the case a whole-buffer feed hides: every chunk makes the
    parser decide whether the reply is complete yet.  Same size as `mget` so
    the chunked and whole-buffer rows are comparable; past ~10 MB per reply
    both libraries become allocator-bound and the ratios turn into noise
    (measured: hiredis drops to 1838 MB/s at 24 MB), so it is deliberately
    not sized up.
    """
    emb = np.arange(1536, dtype=np.float32).tobytes()  # 6144 bytes
    one = b"$%d\r\n" % len(emb) + emb + b"\r\n"
    mget = b"*1000\r\n" + one * 1000
    return mget, mget


CHUNK = 64 * 1024


def materialize_strided(got):
    """One strided view for a uniform array-of-bulk reply (the embedding
    shape).  Payloads are separated by framing (`$6144\\r\\n`, `\\r\\n`), so
    stride != rows * itemsize: a flat view would reinterpret those bytes as
    data from the second row on.
    """
    table, arena = got
    t = np.frombuffer(table, dtype="<i8").reshape(-1, 3)
    if len(t) < 3 or int(t[0][2]) != 42:  # '*'
        return
    count, plen = int(t[0][1]), int(t[1][1])
    if count < 1 or plen <= 0 or len(t) != count + 1:
        return
    if not (np.all(t[1:, 1] == plen) and np.all(t[1:, 2] == 36)):  # all '$'
        return
    off0 = int(t[1][0])
    np.ndarray((count, plen // 4), dtype="<f4", buffer=arena,
               offset=off0, strides=(int(t[2][0]) - off0, 4))


def run_highway_strided(payload, iters):
    reader = hpredis.Reader(highway_mode=True)
    reader.feed(payload)
    t0 = time.perf_counter()
    for _ in range(iters):
        reader.feed(payload)
        while True:
            got = reader.gets()
            if got is False:
                break
            materialize_strided(got)
    return time.perf_counter() - t0


def run_chunked(make_reader, payload, iters, chunk=CHUNK, consume=None):
    """Socket-shaped feed: 64 KiB reads, drain replies after each one.

    Works for classic and highway readers alike: both answer an incomplete
    reply with `False`.  `consume` models what the caller does with each reply.
    """
    reader = make_reader()
    n = len(payload)
    t0 = time.perf_counter()
    for _ in range(iters):
        for pos in range(0, n, chunk):
            reader.feed(payload[pos : pos + chunk])
            while True:
                got = reader.gets()
                if got is False:
                    break
                if consume is not None:
                    consume(got)
    return time.perf_counter() - t0


def bench_ai():
    mget, stream = make_ai_corpora()
    n_emb = 1000

    print("\nMGET of 1000 x 1536-dim float32 embeddings (%d bytes, %.1f MB)"
          " [MB/s and ns/embedding; best of 3 runs; feed+gets+consumer; no socket/server/GPU]"
          % (len(mget), len(mget) / 1e6))
    iters = 200
    rows = [
        ("hiredis classic", best(lambda: run_classic(mget, iters, hiredis.Reader))),
        ("hpredis classic", best(lambda: run_classic(mget, iters, hpredis.Reader))),
        ("hpredis drain  ", best(lambda: run_drain(mget, iters))),
        ("hpredis highway", best(lambda: run_highway(mget, iters))),
        ("hpredis hw+views", best(lambda: run_highway(mget, iters, materialize=True))),
        ("hpredis hw+strided", best(lambda: run_highway_strided(mget, iters))),
    ]
    base = rows[0][1]
    for label, t in rows:
        print("  %-16s %9.0f MB/s  %8.1f ns/embedding  (%s)"
              % (label, len(mget) * iters / t / 1e6, t / iters / n_emb * 1e9,
                 "x%.2f" % (base / t)))

    n_chunks = (len(stream) + CHUNK - 1) // CHUNK
    print("\n64 KiB socket reads of the same 1000-element reply (%d bytes, %.1f MB, %d chunks)"
          " [same unit and statistic; one gets() after each feed; 'whole' = single feed for comparison]"
          % (len(stream), len(stream) / 1e6, n_chunks))
    iters = 30
    rows = [
        ("hiredis classic", best(lambda: run_chunked(hiredis.Reader, stream, iters))),
        ("hpredis classic", best(lambda: run_chunked(hpredis.Reader, stream, iters))),
        ("hpredis highway", best(lambda: run_chunked(
            lambda: hpredis.Reader(highway_mode=True), stream, iters))),
        ("hpredis hw+strided", best(lambda: run_chunked(
            lambda: hpredis.Reader(highway_mode=True), stream, iters,
            consume=materialize_strided))),
        ("hiredis whole  ", best(lambda: run_classic(stream, iters, hiredis.Reader))),
        ("hpredis whole  ", best(lambda: run_classic(stream, iters, hpredis.Reader))),
    ]
    base = rows[0][1]
    for label, t in rows:
        print("  %-16s %9.0f MB/s  (%s)"
              % (label, len(stream) * iters / t / 1e6, "x%.2f" % (base / t)))


def legend():
    print("legend")
    print("  unit       MB/s = payload bytes x iterations / wall seconds (higher is better);")
    print("             ns/embedding = wall nanoseconds per element per iteration")
    print("  statistic  best of 3 timed runs per cell (best-of-N suppresses run-to-run drift)")
    print("  timed      feed() + gets() + the consumer work named in the row")
    print("  excluded   socket reads, Redis server, GPU transfers, process start")
    print("  rows       classic     Reader.gets() -> Python objects (list/bytes/int)")
    print("             drain       hpredis drain() over the whole buffer, one call")
    print("             highway     highway_mode gets() -> (table, arena), no payload read")
    print("             hw+views    highway + one numpy view per payload row")
    print("             hw+strided  highway + one strided numpy view (zero copy)")
    print("             decode      encoding=\"utf-8\" on every payload (decode_responses=True)")
    print("  gc         gc.get_stats() collected deltas, one run each (not best-of-3)")


def main():
    legend()
    print("corpus | reader | speedup | gc(hiredis/hpredis)")
    for name, (payload, iters) in make_corpora().items():
        bench_one(name, payload, iters)
    bench_ai()


if __name__ == "__main__":
    main()
