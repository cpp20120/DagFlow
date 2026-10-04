# Local GitHub snapshot vs current DagFlow

Measured from the actual sources formerly in `dagflowoldfromgithub`, without consulting
its README. Current means the working tree after the park/wake, empty-steal and
scope/completion changes. This is not an isolated before/after measurement of
those changes.

The current runtime wins substantially on graph execution and nested work, but
still loses on empty-task stealing at eight workers and on some external-submit
cases with payload. The old snapshot also fails correctness checks; its passing
timings cannot stand in for equivalent lifetime/completion guarantees.

## Method and artifacts

- Same `bench/runtime_suite.cpp`, with an API-spelling adapter for the snapshot.
  Its runtime sources were copied unchanged. Recursive TaskScope is marked
  unsupported, rather than emulated with a different workload.
- Clang 22.1.8, system allocator, static linkage, Release and ThinLTO profiles.
  CMake builds both runtimes from frozen source copies.
- 1/2/4/8 workers, 4096 payload tasks, nominal 0/1000 ns payload, 9 measured
  repeats and 2 warmups, throughput and latency modes, all 14 scenarios.
- One calibration for both versions: 2.049305 ns/iteration; the 1000 ns cases
  use 488 identical integer-chain iterations. Actual payload duration depends
  on load and CPU state.
- 448 paired points (896 invocations), shuffled deterministically; backend
  order shuffled within pairs. Timed invocations ran sequentially. No other
  agent builds or tests ran during measurements. CPU affinity was inherited
  across all 16 logical CPUs, with eight physical cores; no dedicated-core
  isolation or per-point hardware counters.
- Source and binary hashes, source copies, commands, raw repeats, verification
  failures and full tables are in `out/benchmarks/github-comparison/`.
  `report.md` contains all throughput tables; `comparison.csv` also includes
  latency p99, and `results.jsonl` preserves every invocation.

Fresh graph objects are prepared outside timing for `deep_dag`, `fanout_fanin`
and both token cases on **both** runtimes. `graph_reuse` retains its graph and
tests reuse as-is. `dag_build_run` includes construction. Timings include
`wait_idle()` and packet cleanup; graph payload slots and checksums are verified.

## Selected results

Median microseconds for 4096 tasks, eight workers, Release (no LTO):

| Scenario | Nominal payload | GitHub µs | Current µs | Result |
| --- | ---: | ---: | ---: | --- |
| deep_dag | 0 ns | 719.4 | 135.5 | current 5.31× faster |
| nested_spawn | 0 ns | 495.4 | 167.9 | current 2.95× faster |
| external_handles | 0 ns | 7238.4 | 3837.3 | current 1.89× faster |
| fanout_fanin | 0 ns | 1316.5 | 740.4 | current 1.78× faster |
| steal_heavy | 0 ns | 1228.7 | 2495.2 | current 2.03× slower |
| nested_spawn | 1000 ns | 1794.4 | 622.5 | current 2.88× faster |
| fanout_fanin | 1000 ns | 1577.5 | 622.8 | current 2.53× faster |
| steal_heavy | 1000 ns | 2600.1 | 1747.4 | current 1.49× faster |
| external_handles | 1000 ns | 1940.2 | 2705.2 | current 1.39× slower |
| external_detached | 1000 ns | 1208.9 | 1352.8 | current 1.12× slower |

ThinLTO also exposes empty local-work regressions in passing invocations:
`local_saturated` is 1113.7 vs 2457.8 µs (current 2.21× slower), and
`steal_heavy` is 1164.5 vs 2257.6 µs (current 1.94× slower). These figures
identify remaining overhead; they do not prove which runtime function consumes
the time. Repeated runs on isolated cores are appropriate before interpreting
small differences.

Parallel tokens are an exceptional case: Release / eight workers / empty
payload takes 562325.3 vs 476.1 µs. The old `maybe_dispatch_node()` launches from
the queued token count without subtracting tokens already represented by
scheduled workers; with unlimited concurrency, later workers can schedule
additional workers for the same unclaimed tokens. The source suggests excessive
empty-worker scheduling, so the enormous ratio should not be presented as an
ordinary scheduler speedup. No packet counters were collected for this run.

## Correctness and missing capabilities

| Runtime | Passed | Failed | Skipped |
| --- | ---: | ---: | ---: |
| Current | 440 | 0 | 8 |
| GitHub snapshot | 359 | 49 | 40 |

Eight skips per runtime are stealing with only one worker. The other 32 GitHub
skips are recursive TaskScope, which the snapshot does not implement.

GitHub failures:

- 32/32 `graph_reuse` invocations reject the second run with `cycle detected`.
  The code sets graph state to `Running` and never restores an idle/sealed state;
  subsequent `seal()` returns false even for an edgeless graph.
- 17 invocations fail output verification after `wait_idle()`: six
  `local_saturated`, four `uneven`, four `idle_burst`, three `external_detached`.
  This proves the expected results are not visible at the advertised completion
  boundary, not necessarily that tasks are permanently lost. The code has a gap
  between removing a task from a queue and incrementing `inflight_`, while
  `wait_idle()` uses queue emptiness plus that counter. That is a candidate cause,
  not an instrumented attribution from this run.

Failed points have no speed ratio. The snapshot's runtime was not repaired to
make it pass. The next performance target remains empty-task stealing/local
publication and external submission under payload; graph and nested execution
already show material gains in this matrix.

To reproduce with a new output directory:

```sh
python3 scripts/benchmark_github.py \
  --legacy out/benchmarks/github-comparison/source/github \
  --output out/benchmarks/github-comparison-new
```

For direct CMake builds and runs, see [the CMake workflows](cmake.md).
