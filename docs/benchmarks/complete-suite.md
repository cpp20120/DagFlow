# Complete benchmark run

> **Historical workflow note (October 2026).** This report preserves its original
> benchmark commands and measurements. The former `scripts/benchmark_*.py`
> orchestration is retired; those commands are not part of the current build.
> For reproducible runs use [CMake benchmark campaigns](campaigns.md).


For individual CMake build/run targets, optimization presets and PGO without
Python, see [CMake benchmark workflows](cmake.md).

`scripts/benchmark_all.py` runs the existing benchmark families sequentially and
keeps their outputs under one new directory:

1. `benchmark_suite.py`: Pool submission, queueing, stealing, nested work,
   graph shapes, graph reuse and graph tokens across worker and payload sizes.
2. `benchmark_main.py`: producer contention, batch/shard choices, hot-shard
   skew, overflow, nested helping, mixed work and idle bursts, with O3/LTO
   profiles and per-case provenance.
3. `benchmark_public_api.py` and `benchmar_tbb.py`: public-API workloads for
   DagFlow and oneTBB, followed by a side-by-side report.

Run the full default matrix with:

```sh
python3 scripts/benchmark_all.py --output out/benchmarks/complete-run
```

This is a large measurement run: it builds several binaries and executes every
selected scenario. Narrow it before a focused investigation, for example:

```sh
python3 scripts/benchmark_all.py --suites runtime,stress \
  --workers 1,4 --work-ns 0 --tasks 1024 --stress-profiles o3 \
  --stress-rounds 1 --stress-repeats 5 --output out/benchmarks/focused
```

The runner refuses to reuse an output directory. It records top-level commands
and logs in `commands.json` and `logs/`; each child harness writes its own full
manifest, raw results, and build records. CPU affinity defaults to `inherit` so
worker scaling does not assume that a contiguous set of physical cores is
available. Use `--affinity physical` when the host has enough permitted cores
for each stress case.

The public-API comparison uses the maximum requested worker count. oneTBB's
arena concurrency and DagFlow's worker count are recorded with the result; the
comparison is not a substitute for matching CPU placement and build flags in a
controlled follow-up.
