# STL allocator routing: paired measurements

> **Historical workflow note (October 2026).** This report preserves its original
> benchmark commands and measurements. The former `scripts/benchmark_*.py`
> orchestration is retired; those commands are not part of the current build.
> For reproducible runs use [CMake benchmark campaigns](campaigns.md).


Measured 2026-09-30 on AMD Ryzen 7 6800H, Clang 22.1.8, Linux.

Routing runtime-owned STL vectors through `RuntimeAllocator` substantially reduces
build/seal/run/destruction time with **tbbmalloc** in this workload. The default
mimalloc backend shows no comparable gain. This is a result for allocator routing,
not a comparison against an old scheduler or a claim about all graph shapes.

## Experiment

The runner copies the current working tree twice. `before` reverts only the ten
STL vector allocator uses; `after` retains them. Algorithms, benchmark source,
`runtime_memory` zero-size handling and compiler options are identical. This is a
reconstructed baseline, not a historical commit; the working tree was dirty.
The [exact patch](stl-allocator-routing-data/allocator-routing.patch) and
[source/binary hashes](stl-allocator-routing-data/manifest.json) identify the comparison.

- Six static DagFlow Release builds: system/mimalloc/tbbmalloc × before/after.
  Allocator dependencies remain dynamically linked. No LTO, PGO or native CPU flags.
- 48 configurations: three backends × 1/4 workers × four scenarios × 0/400 payload iterations.
- Five paired process runs per configuration; each process has five warmups and
  31 timed repetitions, with 4,096 payloads each. Total: 480 successful processes.
- Configuration order and before/after order are shuffled with a fixed seed.
  Payload results are checked after each repetition and checksums match across pairs.
- CPU affinity: `0,2` for one worker, `0,2,4,6,8` for four workers, one logical CPU
  per physical core. Threads are free to migrate inside that set; pool pinning is off.
- CPU frequency/boost and host load were not controlled. Five pairs do not establish
  confidence intervals; reported min/max values describe observed pair variation.

`before` and `after` times below are medians of the five process p50 values.
Change is the median of the five **paired** `(after / before - 1)` percentages,
so it need not equal the ratio of the displayed median times. Negative means less time.

## Build, seal, execute and destroy a chain

`dag_build_run` creates a 4,096-node dependency chain on every repetition, runs it
and destroys it within timing. Pool startup is excluded. Four workers do not make
this serial dependency chain four-way parallel.

| Backend | Workers | Payload iterations | Before, µs | After, µs | Paired change | Pair range |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| system | 1 | 0 | 843.0 | 820.6 | -0.3% | -6.1% … +18.4% |
| system | 1 | 400 | 4766.5 | 4740.8 | -0.5% | -1.3% … +0.3% |
| system | 4 | 0 | 955.8 | 907.9 | -2.6% | -9.9% … +2.2% |
| system | 4 | 400 | 5439.2 | 5467.3 | -0.1% | -2.1% … +0.6% |
| mimalloc | 1 | 0 | 251.6 | 249.6 | -1.1% | -2.5% … +3.9% |
| mimalloc | 1 | 400 | 3783.1 | 3832.8 | +0.7% | -0.7% … +1.7% |
| mimalloc | 4 | 0 | 358.2 | 347.6 | -3.0% | -9.4% … -0.8% |
| mimalloc | 4 | 400 | 4388.3 | 4455.5 | +2.7% | -3.6% … +5.4% |
| tbbmalloc | 1 | 0 | 532.5 | 215.9 | -57.3% | -63.3% … -54.4% |
| tbbmalloc | 1 | 400 | 4344.7 | 3781.7 | -12.7% | -13.9% … -9.0% |
| tbbmalloc | 4 | 0 | 628.6 | 338.8 | -47.0% | -54.0% … -45.2% |
| tbbmalloc | 4 | 400 | 4999.9 | 4489.0 | -11.7% | -13.8% … -5.8% |

The tbbmalloc reduction appears in all five pairs: about 57% with one worker and
47% with four at zero payload iterations. With 400 iterations the reduction remains
about 12–13%. System and mimalloc have much smaller changes; this run provides no
basis for claiming a large general speedup for either.

## Controls and limits

`graph_reuse` runs a prebuilt graph of independent nodes. `external_handles` and
`external_detached` submit individual tasks from one external producer. These
scenarios help detect regressions, but do not isolate the modified builder buffers.
Their setup, including pool construction, is outside the timed interval.

At 400 payload iterations most control changes lie near zero. Exceptions include
system `external_handles` at four workers (−4.7%, all pairs negative), and mimalloc
`graph_reuse` at one worker (−4.2%, pair range −5.1% … +0.7%). Do not automatically
attribute such differences to an allocation removed from the timed path: setup
allocation, code layout, scheduling and host noise can also affect execution.

Zero-work controls with four workers are particularly unstable. For example,
system `external_detached` varies from −34.6% to +86.5% across pairs; mimalloc
`graph_reuse` from −54.8% to +2.1%. A single median here is insufficient evidence
for an optimization or regression. All 48 rows, including these results, are in
[summary.csv](stl-allocator-routing-data/summary.csv).

This matrix does not measure pool startup, `Config` copying, `GraphScope` block
construction, completion dependency composition, retained RSS, or allocation counts.
It does not establish performance for every graph shape or worker count.

## Why mimalloc differs

The installed `/usr/lib/libmimalloc.so.3.5` exports global malloc/free and C++ new.
A separate `LD_DEBUG=bindings` run of **mimalloc-before** confirms that the executable
and libstdc++ resolve these symbols to mimalloc. Thus ordinary STL allocation in
that baseline already uses mimalloc on this machine. The explicit adapter mostly
changes the entry path, rather than switching STL from libc allocation to mimalloc.

[Loader evidence](stl-allocator-routing-data/mimalloc-bindings.txt) was collected
outside timing. DagFlow itself defines no global new/delete override; the linked
allocator library can nevertheless supply one. Results depend on its build and
linkage configuration, so do not generalize this interposition to every installation.

## Hardware-counter probe

A separate before/after tbbmalloc probe used one worker, CPUs `0,2`, zero payload
iterations, 401 repetitions and ten warmups. `perf stat` covers the entire process,
including startup, validation and warmup; these are not counters for only the timed
regions of the primary experiment. This was one corroborating pair, not the five-pair
statistical comparison above. Hardware events were restricted to userspace (`:u`).

| Counter | Before | After |
| --- | ---: | ---: |
| cycles:u | 437,564,897 | 372,355,241 |
| instructions:u | 1,051,482,218 | 1,048,159,438 |
| cache-misses:u | 14,102,741 | 13,318,864 |
| page-faults:u | 96,735 | 1,137 |

Instructions barely change while faults fall sharply. This is consistent with
allocator memory reuse reducing page mapping/fault overhead in repeated graph
construction. It does not identify which buffer faults or distinguish the full
allocation/syscall mechanisms; that would require a targeted allocation or fault
trace. Userspace cycle counts also exclude kernel fault handling.

Exact commands and original counter output are preserved with the data.

## Reproduce and inspect

```sh
python3 scripts/benchmark_allocators.py \
  --output out/benchmarks/stl-allocator-routing-repeat \
  --trials 5 --repeats 31 --warmup 5
```

Requires Linux, five available physical cores, Python 3, CMake, Ninja, Clang and
installed mimalloc/tbbmalloc. Output must be a new directory. The baseline recipe
intentionally fails if the allocator uses change instead of silently benchmarking
a different patch.

[Current runner](campaigns.md),
[raw process results](stl-allocator-routing-data/paired.jsonl),
[summary JSON](stl-allocator-routing-data/summary.json),
[manifest](stl-allocator-routing-data/manifest.json), and
[perf commands](stl-allocator-routing-data/perf-commands.json) are retained here.
Local `out/benchmarks/stl-allocator-routing/` additionally contains both complete
source snapshots, six builds, every invocation and stdout/stderr log. That output
directory is ignored by Git; preserve it to rerun the exact source snapshot after
future code changes.
