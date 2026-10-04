# Memory layer and scheduler topology: local comparison

Historical snapshot before the [wakeup follow-up](wakeup-profile.md).

Measured 2026-09-30. This change establishes contiguous ownership and explicit
logical domains; it does **not** deliver a uniform throughput improvement.
Tiny external submissions and idle bursts regress in this local comparison.

## What changed

- Scheduler owns `Local[]`, `Shard[]`, and CSR membership: three bulk allocations
  through `runtime_memory`, independent of worker count.
- ParkingLot owns `Waiter[]`, domain ranges and idle bitmap words: three more
  bulk allocations. These six exclude the two owning objects, OS thread/CV
  resources and the pool's thread/configuration containers.
- Explicit `home_shard` drives ingress, affinity hints, stealing and wakeup.
  `Config::worker_shards` optionally supplies membership; CPU placement is separate.
- An 8-byte SplitMix64 state replaces each worker's `mt19937` state.
- Mimalloc naturally aligned requests use its native small-object entry point;
  arbitrary/over-aligned buffers retain aligned allocation. Backends own size
  classes and cross-thread free. There is no additional DagFlow slab allocator.

The [contract](../scheduler-topology.md) describes mapping, priority tiers,
batch publication, exception cleanup and the lost-wakeup protocol.

## Method

Linux x86_64, AMD Ryzen 7 6800H (8 cores/16 threads), Clang 22.1.8, mimalloc,
static Release, no LTO/PGO/native tuning. Pinning is disabled. The baseline is a
binary built from the working tree before this memory/topology change, not a
comparison against repository HEAD. Both binaries use the existing benchmark
suite. Binary hashes and final source hashes are recorded in the manifest.

For each point: 4,096 task units, two warmups, nine measured repetitions. Three
paired trials alternate before/after order; points are deterministically shuffled.
The table uses the median of each version's three run medians. Checksums agree
between versions. Workers are 1 or 4, with one shard per worker. Task bodies use
exactly 0 or 400 iterations in both binaries; the nominal `work_ns=1000` label
is not a promise that 400 iterations take exactly 1 microsecond.

These are throughput-mode whole-run measurements, not submission latency or
p99/p999 claims. No frequency lock, CPU isolation, PMU attribution or statistical
confidence interval was established. Small differences should not be treated as
stable wins. This is a local diagnostic comparison, not a general performance
ranking or a completed scaling study.

## End-to-end results

Four workers; **after/before below 1 means less elapsed time**.

| Scenario | Before, 0 iterations (µs) | After, 0 iterations (µs) | Ratio, 0 iterations | Ratio, 400 iterations |
|---|---:|---:|---:|---:|
| `external_detached` | 1593.2 | 2239.2 | 1.405 | 1.002 |
| `external_handles` | 2662.0 | 3497.1 | 1.314 | 0.995 |
| `idle_burst` | 1935.4 | 2960.2 | 1.530 | 1.013 |
| `local_saturated` | 1005.2 | 811.0 | 0.807 | 0.996 |
| `steal_heavy` | 1097.7 | 934.1 | 0.851 | 0.947 |
| `nested_spawn` | 493.1 | 397.2 | 0.805 | 0.769 |
| `scope_recursive` | 962.2 | 851.4 | 0.885 | 0.854 |
| `graph_reuse` | 2237.1 | 2566.8 | 1.147 | 0.973 |

At one worker, empty external submissions regress about 11–13%, and local
saturation about 11%; the complete 30-point summary includes these results.
There is no universal speedup. Four-worker external submission is approximately
unchanged at 400 iterations, while nested spawn and recursive scope still improve.
The tiny-task external/wakeup regression remains unresolved and needs profiling
before calling the runtime performance work finished.

## Allocation and layout experiments

A separate single-thread microbenchmark compares the **actual before/after
`allocate_bytes` adapters**, including their dispatch, followed by free. Each
sample performs 2,000 batches of 256 simultaneously live blocks, writes one byte
per block, and reports the median of eleven alternating samples after warmup.
Alignment uses the actual type; numbers include allocation and free per object.

| Request | Size/alignment | Before (ns) | After (ns) | After/before |
|---|---:|---:|---:|---:|
| `ScheduledTask` | 96/16 | 11.453 | 6.757 | 0.590 |
| `CompletionState` | 168/8 | 8.667 | 9.153 | 1.056 |

The packet path benefits here; completion-state dispatch does not show a win.
This is neither a remote-free throughput test nor an application-level speedup.
Remote frees, including after the allocating thread exits, are correctness-tested.

Cache-line padding was tried for idle words, waiters, and both together. Paired
runs showed mixed results, without a consistent improvement across external,
graph and local scenarios. The final code retains natural alignment for these
arrays. Padding experiment records are retained separately; they do not describe
the final binary. The final allocator eligibility check uses a bit mask instead
of division, with the documented nonzero power-of-two alignment precondition.

## Validation

Final-code checks passed:

| Configuration | Passed | Scope |
|---|---:|---|
| system Debug | 16/16 | Full configured suite, including allocation-failure injection |
| mimalloc Debug | 16/16 | Full configured suite, including benchmark harness |
| tbbmalloc Debug | 15/15 | Full configured suite, benchmark harness disabled |
| ASan + UBSan, system | 17/17 | Full configured suite; leak detection disabled |
| TSan, system | 7/7 | Topology/allocation, scheduler fairness, queues, memory, TaskScope, benchmark harness |

The new tests cover arbitrary and empty shard groups, multiword idle masks,
signals before waits, publication/registration races, long-backoff bursts,
cross-domain children, batch visibility, six construction failure points and
partial Pool/thread startup cleanup. Passing sanitizer/race tests supports the
implementation but is not an exhaustive proof of all interleavings.

## Local artifacts

`out/benchmarks/memory-topology/` contains:

- `paired.jsonl`, `summary.json`, `manifest.json`, and `binaries/{before,after}`
  for the final comparison;
- `compare.py` with exact commands and pairing order;
- `memory-adapter.cpp`, `memory-before.cpp`, `memory-after.cpp`, and
  `memory-adapter.txt` for the adapter experiment;
- `tests-*.log` for final checks;
- `initial-packed/` and `*-padding.jsonl` for earlier experiments.

These generated artifacts are local and ignored by Git. The comparison script
uses local build paths; replace them with preserved binaries when replaying.
The adapter experiment compiles the before implementation with symbol renames
(`allocate_bytes=before_allocate_bytes`, likewise for deallocation and backend
name), compiles the after implementation normally, then links both with
`memory-adapter.cpp` and `-lmimalloc`, using `-std=c++23 -O3 -Iinclude -Isrc`.
