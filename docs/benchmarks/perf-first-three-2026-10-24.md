# First three runtime performance experiments — 2026-10-24

Measured batch/range granularity, TaskGraph execution budget, and scheduler
stealing/ingress settings against a fresh baseline. Production runtime defaults
remain unchanged. Batch submission has a repeatable throughput benefit but a
start-latency cost. Graph and scheduler constants have workload-dependent
tradeoffs; the measurements do not justify a universal replacement.

## Reproduction and measurement conditions

- Source revision: `d51a845`, plus the range benchmark additions in this working
  tree. The exact pre-experiment tracked source snapshot, file hashes and patch
  are retained in the experiment directory below.
- AMD Ryzen 7 6800H, 8 physical cores / 16 logical CPUs; Clang 23.1.3, CMake 4.4.4,
  Ninja, C++23, `bench-o3`, system allocator, static library. Compiler flags include
  `-O3 -DNDEBUG -g`; runtime diagnostics, LTO, PGO and native tuning are disabled.
- Cases use `taskset` with physical CPU representatives `0,2,4,6,8,10,12,14`:
  the first `min(workers+1,8)` CPUs. Threads may migrate within this set. At eight
  workers the caller shares the eight cores. No CPU isolation or fixed-frequency
  control was applied; boost remains enabled. Results describe this machine.
- Explicit baseline settings: central drain batch 1024, shards equal to workers,
  individual submission, range chunk 16384, WS grain hint 16384, graph budget 64,
  steal batch 4, external poll interval 32. The benchmark CLI's historical
  central-batch default is 32, so relying on that default would measure a different
  Pool configuration.
- Fixed payload iterations 0 and 64; 8192 tasks per graph/scheduler/submission
  workload, 256 for idle bursts after a 2 ms sleep, 131072 elements for ranges.
  Zero iterations still perform the benchmark's result write and task machinery.
  Uneven workloads give every 16th task 32 times the payload iterations.
- Screening: 320 cases, three measured process rounds plus one warmup round;
  each process has nine measured iterations plus two warmups. Cases are shuffled
  per round with seed 20261024. This covers 1/4/8 workers for batch/range/graph
  and 4/8 workers for scheduler settings.
- Confirmation: 162 cases, five measured process rounds plus one warmup round;
  21 measured iterations and three warmups in each process. Throughput and latency
  are separate cases. Latency samples use stride 17, avoiding alignment with the
  16-element uneven-payload period.
- Each invocation validates every result slot. Cross-variant, cross-mode and
  equivalent-payload cross-scenario checksums match. All 2252 processes, including
  warmups, completed successfully; 1770 are measured processes. Binary hashes
  were checked again after measurement. Builds and tests did not overlap timing.

Artifacts: `out/experiments/perf-first-three-20261024/`. This ignored directory
contains `manifest.json`, `source.patch`, source snapshots, variant test/build
logs, `screen-cases.json`, `confirm-cases.json`, the CMake harness configurations,
raw `screen/` and `confirm/` outputs, analysis JSON/text and `verification.json`.
`out/build/perf-first-three/` contains the binaries. The harness hashes its small
launcher; the experiment manifest separately hashes every actual benchmark binary
and source file. Cases retain the explicit CPU sets and binary paths.

Run from the repository root, using a fresh experiment/output directory when
preserving previous measurements:

```sh
# prepare.py builds isolated source variants and records test outcomes.
python3 out/experiments/perf-first-three-20261024/prepare.py
# Adapt only size-specific ordering expectations in experimental steal tests.
python3 out/experiments/perf-first-three-20261024/adapt_topology.py
python3 out/experiments/perf-first-three-20261024/cases.py
cmake -DHARNESS_CONFIG="$PWD/out/experiments/perf-first-three-20261024/screen.cmake" \
  -P cmake/dagflow/benchmark/HarnessCMake.cmake
python3 out/experiments/perf-first-three-20261024/confirm.py
cmake -DHARNESS_CONFIG="$PWD/out/experiments/perf-first-three-20261024/confirm.cmake" \
  -P cmake/dagflow/benchmark/HarnessCMake.cmake
python3 out/experiments/perf-first-three-20261024/analyze.py screen
python3 out/experiments/perf-first-three-20261024/analyze.py confirm
python3 out/experiments/perf-first-three-20261024/verify_results.py
```

The local Python scripts prepare isolated sources/cases and summarize artifacts;
process execution uses the existing CMake harness. They are experiment artifacts,
not new build-system dependencies. Build flags and full arguments are in the
manifest and preparation script. No benchmark infrastructure was changed.

## Tested variants and correctness checks

| Direction | Variants against baseline |
| --- | --- |
| Submission | Batch 4, 16, 64 versus individual submission |
| Static ranges | Chunk 4096 and 65536 versus 16384 |
| Work-stealing ranges | Grain hint 1024 and 65536 versus 16384 |
| TaskGraph | Budget 16 and 256 versus 64 |
| Scheduler | Steal batch 1 and 16 versus 4; poll interval 8 versus 32; central drain 32 and 128 versus 1024; one shard versus worker-count shards |

Baseline passed eight selected CTests: runtime suite smoke/schema, graph tests,
reference-graph model property tests, scheduler topology/fairness, pool queues,
and API convenience. Each variant passed its relevant subset, except budget 256.
The current/legacy campaign generator registration check and `git diff --check`
also passed. This experiment did not rerun the full sanitizer/weak-memory suite.

Budget 256 fails `graph_budget_allows_priority_progress()` in `graph_tests.cpp`:
a high-priority task submitted by the first graph invocation has not run by the
join after 129 tokens. The test also contains a 128-empty-transition path, which
this aborted run does not reach. This violates the existing bounded
priority-progress regression. It is excluded from timing, not treated as a faster
candidate. Budget 16 passes both paths of the same check.

The original topology test encodes the exact acquisition order for steal batch 4.
Both steal variants therefore initially fail that expectation. In isolated test
copies only, expected sibling order and publication flags were changed for the
selected size; the remaining custody/publication/recruitment checks were retained
and passed. Original failure logs, adaptation patches and successful test logs
are retained. Production tests and runtime sources were not edited for this.

## Confirmation: total completion time

Values below come only from the confirmation series. Time columns are medians of
five process-level `run_p50_us` values. A ratio is the median of five candidate /
baseline ratios paired by process round, with their observed minimum and maximum;
therefore it need not equal the ratio of the two displayed medians. Lower is
better. The bracket is an observed range, not a confidence interval. Full results
including all controls are in `confirm-analysis.json`; this table selects the
main benefits, regressions and inconclusive comparisons.

| Case / candidate | Baseline µs | Candidate µs | Paired time ratio, median [min, max] |
| --- | ---: | ---: | ---: |
| `batch/external_detached/w4/i0/batch64` | 3203.38 | 1702.81 | 0.531 [0.406, 0.698] |
| `batch/external_detached/w8/i0/batch64` | 3504.84 | 2182.31 | 0.636 [0.452, 0.871] |
| `batch/external_detached/w8/i64/batch64` | 3245.65 | 2475.70 | 0.762 [0.532, 0.886] |
| `batch/idle_burst/w8/i0/batch64` | 290.49 | 111.76 | 0.385 [0.307, 0.422] |
| `range/range_static/w8/i64/chunk4096` | 2419.33 | 2374.38 | 1.022 [0.768, 1.033] |
| `range/range_static_uneven/w8/i64/chunk4096` | 7008.01 | 7749.07 | 1.059 [0.980, 1.120] |
| `range/range_ws/w8/i64/grain1024` | 2547.97 | 2234.74 | 0.851 [0.645, 1.238] |
| `range/range_ws_uneven/w8/i64/grain1024` | 8831.16 | 7198.74 | 0.880 [0.784, 1.135] |
| `range/range_ws_uneven/w8/i64/grain65536` | 8831.16 | 49163.92 | 5.567 [5.420, 6.904] |
| `graph/deep_dag/w4/i0/budget16` | 128.74 | 343.17 | 2.683 [2.530, 2.804] |
| `graph/deep_dag/w8/i0/budget16` | 190.16 | 421.23 | 2.215 [2.015, 2.461] |
| `graph/deep_dag/w8/i64/budget16` | 1646.46 | 1242.68 | 0.759 [0.710, 0.957] |
| `graph/graph_tokens_serial/w8/i0/budget16` | 118.60 | 258.28 | 2.531 [1.969, 3.118] |
| `graph/graph_tokens_serial/w8/i64/budget16` | 1569.77 | 1280.24 | 0.793 [0.762, 0.899] |
| `scheduler/external_detached/w4/i64/shards1` | 3380.11 | 2106.40 | 0.635 [0.527, 0.712] |
| `scheduler/external_detached/w8/i64/shards1` | 3295.91 | 3002.18 | 0.943 [0.612, 1.290] |
| `scheduler/uneven/w4/i64/steal1` | 2244.23 | 2688.92 | 1.163 [1.149, 1.228] |
| `scheduler/uneven/w8/i64/shards1` | 4342.90 | 4426.09 | 1.016 [0.812, 1.082] |
| `scheduler/steal_heavy/w8/i64/steal1` | 4573.48 | 4112.16 | 0.905 [0.882, 0.971] |
| `scheduler/local_saturated/w8/i0/poll8` | 5048.86 | 5063.62 | 1.015 [0.980, 1.027] |
| `scheduler/uneven/w8/i64/poll8` | 4342.90 | 4330.86 | 0.997 [0.930, 1.024] |

## Confirmation: sampled start latency

This is p99 across sampled tasks within each process, summarized across five
processes. It is not the p99 of nine or 21 whole-run timings. Submission latency
starts when the callable is constructed, including batch assembly for batched
submission; graph/range latency starts at graph/range operation start. These
measure different quantities and should not be compared across API families.
The workloads submit as fast as possible, without a fixed arrival-rate model.
A candidate changes offered load as well as service behavior, so these are
saturation-test tradeoffs, not predictions of a production latency SLO.

| Case / candidate | Baseline start-latency p99 µs | Candidate p99 µs | Paired p99 ratio, median [min, max] |
| --- | ---: | ---: | ---: |
| `batch/external_detached/w4/i0/batch64` | 9.95 | 54.03 | 5.087 [3.282, 7.317] |
| `batch/external_detached/w8/i0/batch64` | 6.53 | 61.22 | 10.219 [7.634, 12.746] |
| `batch/external_detached/w8/i64/batch64` | 7.75 | 72.69 | 9.386 [4.117, 10.450] |
| `batch/idle_burst/w8/i0/batch64` | 14.14 | 85.95 | 6.215 [5.580, 6.750] |
| `range/range_static/w8/i64/chunk4096` | 3466.29 | 2469.45 | 0.767 [0.662, 0.815] |
| `range/range_static_uneven/w8/i64/chunk4096` | 7920.40 | 6915.12 | 0.961 [0.769, 0.984] |
| `range/range_ws/w8/i64/grain1024` | 3654.56 | 2465.98 | 0.684 [0.627, 0.733] |
| `range/range_ws_uneven/w8/i64/grain1024` | 7999.15 | 7059.80 | 0.917 [0.777, 1.064] |
| `range/range_ws_uneven/w8/i64/grain65536` | 7999.15 | 49244.35 | 6.146 [5.461, 6.832] |
| `graph/deep_dag/w4/i0/budget16` | 231.94 | 366.25 | 1.520 [1.126, 1.666] |
| `graph/deep_dag/w8/i0/budget16` | 256.60 | 466.00 | 1.815 [1.634, 2.138] |
| `graph/deep_dag/w8/i64/budget16` | 2229.79 | 1484.30 | 0.639 [0.594, 0.904] |
| `graph/graph_tokens_serial/w8/i0/budget16` | 172.10 | 313.37 | 1.665 [1.638, 2.047] |
| `graph/graph_tokens_serial/w8/i64/budget16` | 1973.58 | 1747.65 | 0.865 [0.821, 0.977] |
| `scheduler/external_detached/w4/i64/shards1` | 14.45 | 1410.56 | 101.086 [58.832, 138.209] |
| `scheduler/external_detached/w8/i64/shards1` | 8.10 | 2521.87 | 344.086 [245.821, 404.206] |
| `scheduler/uneven/w4/i64/steal1` | 1478.77 | 675.46 | 0.457 [0.228, 0.565] |
| `scheduler/uneven/w8/i64/shards1` | 12.42 | 8.23 | 0.766 [0.602, 0.885] |
| `scheduler/steal_heavy/w8/i64/steal1` | 10.68 | 10.15 | 0.953 [0.750, 3.688] |
| `scheduler/local_saturated/w8/i0/poll8` | 11.20 | 10.84 | 0.960 [0.900, 44.325] |
| `scheduler/uneven/w8/i64/poll8` | 12.42 | 10.99 | 0.885 [0.647, 1.141] |

## Decisions

1. **Batch submission is the clearest throughput opportunity.** Batch 64 reduces
   completion time in all five confirmation rounds for the listed submission
   cases: median paired reductions of 47% at four workers and 36% at eight workers
   for zero payload, and 62% for the small idle burst. Sampled start latency rises:
   e.g. 6.53 → 61.22 µs p99 at eight workers. Prefer explicit batching where callers
   naturally have a batch and can accept queueing; do not automatically batch every
   latency-sensitive submission. Batch 16 is also available in the full results.
2. **Grain should be workload-dependent.** A 1024 hint at eight workers becomes
   effective grain 2048 because of the `n/(threads*8)` floor. Its median throughput
   improvement is not consistent across all confirmation rounds, although the
   uniform-range sampled p99 improves consistently. Static chunk 4096 has no
   consistent completion-time benefit. Hint 65536 yields only one initial WS tile
   for 131072 elements (`n/grain < 4`) and no subsequent split at `n == 2*grain`;
   it loses parallelism and makes the uneven case about 5.6 times slower. Keep the
   defaults; investigate size/payload-aware grain selection separately.
3. **Keep TaskGraph budget 64.** Budget 16 makes short chains 2.2–2.7 times slower
   and serial zero-payload tokens about 2.5 times slower, while reducing time by
   about 21–24% for the measured heavier eight-worker chain/token workloads.
   These timings establish a tradeoff, not its microarchitectural cause. Further
   work should profile continuation, migration and waiting costs before designing
   an adaptive policy. Budget 256 fails the existing priority-progress check.
4. **Keep scheduler defaults.** One shard reduces the four-worker external-submit
   completion time by a median paired 36%, but its sampled p99 jumps from 14.45 µs
   to 1410.56 µs. The eight-worker throughput result is inconsistent. Steal batch 1
   improves the eight-worker steal-heavy case by about 9.5%, but worsens the
   four-worker uneven case by about 16%. Polling every eight acquisitions produces
   no consistent completion-time improvement in the confirmed local/uneven cases.
   Single-producer results cannot establish multi-producer ingress scalability;
   overflow contention, explicit mixed-priority latency, NUMA and other CPUs need
   separate workloads before any global policy change.

Added persistent benchmark support: four static/WS uniform/uneven range scenarios,
positive `--grain`, reported configured/effective grain and chunk, checksum/schema
coverage including non-divisible ranges, and current-runtime campaign registration
with legacy exclusion. Runtime algorithms and defaults remain unchanged.
