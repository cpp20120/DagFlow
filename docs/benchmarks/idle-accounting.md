# Single-writer pool accounting: local comparison

Measured 2026-09-30. Baseline is the compact-bitmap wakeup implementation from
[the preceding profile](wakeup-profile.md), immediately before replacing the
pool-wide `outstanding_` counter. This is not a comparison against repository
HEAD or the implementation before the topology rewrite.

The [accounting contract](../idle-accounting.md) documents publication, retirement,
producer lifetime, idle observation, notification and bounds.

## Implementation and cost

The shared increment/decrement on every task is replaced by release stores into
single-writer lanes. A worker writes its own publication and retirement counts;
each external producer writes its own registered publication count. Physical
retirement remains after callable destruction and completion propagation.
Stealing adds no packet metadata or counter transfer.

`wait_idle()` pays for registration exclusion and a retirement/publication/
retirement scan. Registration takes a mutex and one producer-node allocation on
first use; a four-slot TLS cache avoids it on hits and reuses registrations after
eviction. Lanes remain alive until pool destruction, including after producer
exit. Registry size is proportional to distinct submitting external threads,
not merely currently running producers.

The final worker updates are inline. Disassembly confirms that packet retirement
uses an ordinary load/increment/store rather than the former locked decrement
and global notify path. This is specifically pool accounting: ingress queue CAS,
round-robin selection, completion-credit atomics and the parking fence remain.

## Method

Linux x86_64, Ryzen 7 6800H, Clang 22.1.8, mimalloc, static Release, no LTO/PGO/
native tuning. Pinning is disabled. Three paired trials alternate binary order;
30 points are deterministically shuffled. Each point has 4,096 task units, three
warmups, 21 measured repetitions, and exactly 0 or 400 body iterations.
All corresponding checksums match. Ratios use the median of the three run medians.

Results measure whole-run throughput, including the scenario's wait, not isolated
submit latency. External scenarios use one producer. No frequency lock, CPU
isolation or confidence interval was established. Multi-producer correctness is
tested, but multi-producer scaling is not established by these measurements.
Warmup includes initial producer registration; cold registration is not part of
the reported steady-state benefit.

## Results

After/before below 1 means less elapsed time.

| Scenario | 1 worker, 0 iterations | 4 workers, 0 iterations | 4 workers, 400 iterations |
|---|---:|---:|---:|
| `external_detached` | 0.743 | 0.751 | 1.063 |
| `external_handles` | 0.865 | 0.908 | 0.984 |
| `idle_burst` | 0.471 | 0.841 | 1.023 |
| `local_saturated` | 1.130 | 0.730 | 0.984 |
| `nested_spawn` | 1.003 | 0.742 | 0.983 |
| `scope_recursive` | 0.965 | 1.060 | 1.059 |
| `graph_reuse` | 0.902 | 0.775 | 0.927 |
| `steal_heavy` | Skipped | 0.792 | 1.056 |

For four-worker zero-body work, detached time fell from 2,013.4 to 1,511.5 µs
(about 25%), handles from 2,693.8 to 2,447.2 µs (about 9%), and graph reuse from
2,067.0 to 1,601.1 µs (about 23%). Local saturation improved about 27%.

There are remaining regressions: single-worker zero-body local saturation is
about 13% slower, four-worker recursive scope about 6% slower. At 400 iterations,
some four-worker points are about 6% slower. The shared-counter removal is not a
universal speedup; extra lane addressing, checks and idle observation have costs.
The causal share of those costs in individual regressions has not been isolated.

An initial version with out-of-line worker accounting operations showed a larger
single-worker local regression. Those calls were inlined before the final
comparison; initial results are archived separately and are not mixed with the
final table.

## PMU and assembly

A separate three-trial alternating `perf stat` comparison uses external detached,
four workers, 4,096 tasks, 20 warmups and 500 measured runs. Counters are user-mode
process totals across all threads, including setup and warmup. Median ratios:

| Counter | After/before |
|---|---:|
| `cycles:u` | 0.863 |
| `instructions:u` | 1.075 |
| `branches:u` | 1.073 |
| `branch-misses:u` | 0.958 |
| `cache-misses:u` | 0.788 |
| `L1-dcache-load-misses:u` | 0.825 |

Cycles and cache misses fell even though instruction count increased. This is
consistent with avoiding shared-counter contention, rather than simply executing
fewer instructions. It is not cache-line ownership attribution; no new c2c
capture was available. `disassembly.txt` records the final generated code.

## Validation

- system: 18/18 configured tests;
- mimalloc: 18/18;
- tbbmalloc: 17/17;
- ASan/UBSan, system: 19/19, leak detection disabled;
- TSan, system: nine targeted accounting/topology/scheduler/queue/memory/scope/
  benchmark tests.

After expanding the handoff test, the changed test was rebuilt and rerun on all
five configurations. It passes 30 chains of 1,000 handoffs with no persistent
root task anchoring the count. The final child remains gated so an early false
idle return is observable. Other new checks cover multiple waiters, concurrent
producer registration/exit, destructor-spawned children, pool address reuse,
cache eviction, producer-allocation failure and 3,000 retirement/wait races.
Long worker backoff checks that progress does not rely on parking timeouts.

These checks support correctness but do not constitute exhaustive interleaving
or weak-memory hardware validation. The existing shutdown precondition remains.

## Artifacts

Local, Git-ignored `out/benchmarks/accounting/` contains preserved before/after
binaries, `compare.py`, `paired.jsonl`, `summary.json`, binary/source hashes in
`manifest.json`, PMU commands and CSV files, `perf-summary.json`, disassembly,
final handoff-test logs and archived initial results. The comparison can be
replayed with `python3 out/benchmarks/accounting/compare.py` using those binaries.
Consumers must rebuild for the changed Pool layout.

## Archived worker-lane cache experiment

The TLS worker-lane pointer cache was measured and removed after mixed results:
about 2% faster single-worker local work versus about 3–5% slower four-worker
local work in the longer series. The [separate archive](../experiments/worker-lane-cache.md)
contains the implementation patch, complete measurement notes and artifact paths.
The single-writer accounting implementation described above remains active.
