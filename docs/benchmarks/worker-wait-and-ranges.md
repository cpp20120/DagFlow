# Worker waits and forward ranges

This change reduces forward-iterator traversal and CPU consumption during
waiting. It does **not** establish a general improvement in noop throughput.
Baseline: `3d37328`; measurements: 2026-10-11 on Ryzen 7 6800H, Clang 22.1.8,
Release `-O3 -DNDEBUG`, system allocator, no LTO/native flags, pinning disabled
in both benchmark versions. The final default idle maximum is **1 ms**.

## Changes and correctness

- `for_each` advances chunk boundaries incrementally. Forward iterators now
  require linear traversal rather than restarting from `begin` for each chunk.
- Worker waits help while work is available, then register for completion,
  prepare parking, check readiness, scan again, and park. New work and final
  completion both wake the worker. Completion records form an intrusive list:
  multiple workers and nested wait frames can observe the same state safely.
  Registration/removal/notification share the completion mutex and gate. The
  final credit signals after publishing readiness. Unregistered completions
  retain the existing lock-free terminal fast path.
- `worker_wait_depth()` diagnoses native-stack nesting. `WorkerWaitPolicy::Reject`
  rejects explicit unfinished waits from that pool's tasks, including cross-pool
  waits. This is not a stackless executor: range publication rollback still
  drains accepted work to protect borrowed objects. Joining scope destructors
  belong outside Reject-mode tasks because rejection there can terminate.
- External queue saturation yields briefly, then sleeps with exponential
  requested delays of 1–128 microseconds. Actual OS sleep can be longer.
  Capacity is checked by timeout; there is no new notification on every pop.
- Idle maximum increases from 200 microseconds to 1 millisecond. Thread pinning
  defaults to false until placement handles allowed CPUs and platform topology.
- Detached exceptions can reach a configured thread-safe noexcept handler.
  Null preserves the previous discard behavior. Blocking user operations still
  have no compensating workers; the lifecycle documentation states this limit.
- All previously disabled fuzz windows are enabled. Wake-protocol replay and
  external-backpressure fuzz coverage are restored. Completion's targeted wake
  suppresses the blocking wake-claim callback while holding the completion mutex;
  dedicated prepare/before-CV gates exercise completion and nested-wait races.
- A one-shard selection avoids the unnecessary round-robin atomic operation.
  Multi-shard publication and the wake fence remain unchanged.

`Config` and completion layout changed: rebuild clients. On the measured ABI,
`CompletionState` grows from 120 to 128 bytes. No custom allocator or lazy error
allocation was introduced.

## Timing boundaries and repeated measurements

Five paired processes per comparison ran in randomized order. Forward-range
processes used one warmup and seven measurements; each incremented all 1,048,576
elements of an existing forward list and waited for completion and pool idle.
Each idle/wait process measured three 500 ms sleep windows using process CPU
time. The waiting probe has one worker waiting on an externally completed
handle; the idle probe has four workers. Noop processes used one warmup and
three measurements of the existing million-task detached benchmark.

CPU sets were `0,2` for one-worker noop, `0,2,4,6,8` for four-worker noop and
all standalone probes, and `0-15` for eight-worker latency runs. These restrict
placement, but do not isolate CPUs. Background activity, SMT and frequency
scaling remain uncontrolled. Benchmarks ran without concurrent builds/tests.

The table uses the median of each process's summary (median for probes, mean
for noop). Ratios are medians of five paired ratios. Intervals are exact
percentile paired-bootstrap intervals over those five blocks, not independent
resampling of repetitions within a process. With only five blocks these are
exploratory intervals, not portable performance guarantees.

| Workload | Before | After | Paired ratio | Exploratory 95% interval |
|---|---:|---:|---:|---:|
| Forward-list `for_each`, 1 worker | 50.141 ms | 4.491 ms | 0.090 | 0.081–0.098 |
| Idle CPU / 500 ms, 4 workers | 63.641 ms | 16.927 ms | 0.264 | 0.255–0.286 |
| Waiting-worker CPU / 500 ms | 497.005 ms | 4.699 ms | 0.0095 | 0.0086–0.0108 |
| Million detached noops, 1 worker | 154.118 ms | 151.594 ms | 0.992 | 0.899–1.013 |
| Million detached noops, 4 workers | 383.586 ms | 361.881 ms | 0.892 | 0.553–1.594 |

The forward-range result is about 11× faster. Idle process CPU falls about 73%,
and waiting-worker CPU about 99%. Neither noop interval establishes an improvement.
Four-worker noop process means span 229–546 ms before and 295–487 ms after;
earlier rounds even reversed the apparent direction.

## Latency tradeoff

The runtime-suite `idle_burst` scenario submitted 256 empty tasks after 20 ms
of quiet time, with two warmups, 21 measured bursts, five process pairs and
latency sampling of every task. Values below are medians of process quantiles.
Task latency is submit-to-start; run duration measures the whole burst.

| Workers | Task p99 before / after | Run p50 before / after | Run p99 before / after |
|---|---:|---:|---:|
| 1 | 139 / 109 µs | 206 / 209 µs | 288 / 269 µs |
| 4 | 25 / 18 µs | 189 / 321 µs | 294 / 377 µs |
| 8 | 15 / 16 µs | 330 / 375 µs | 462 / 613 µs |

Burst completion regressed in these four/eight-worker samples. Lower waiting
CPU does not imply uniformly lower latency. Applications prioritizing rapid
burst completion can retain `idle_us_max = 200` and measure their workload.

A separate timeout-only experiment on the changed runtime compared 200 µs,
1 ms and 10 ms, using the same four-worker burst workload and five processes.
Task p99 was 21.5 / 27.0 / 178.6 µs; burst p50 was 196.6 / 216.5 / 217.1 µs.
This ruled out 10 ms as the default. The retained 1 ms is a CPU/latency compromise,
not a universal optimum. Timers remain a fallback; deterministic fuzz cases use
very long timeouts to detect lost completion or task-publication wakeups.

## Experiments not retained

The preceding allocator audit on the unchanged source measured four-worker
million-noop means of roughly 538 ms (system), 202 ms (mimalloc), 226 ms
(tbbmalloc), and 106 ms (oneTBB). System results were especially variable.
Changing the backend matters, but includes locality and reuse effects and does
not isolate malloc cost. TBB's participating application thread also differs
from DagFlow's external producer plus workers. This is not a same-contract
scheduler comparison or a result for the final patch.

Perf samples from that audit put system time heavily in parking/search;
with mimalloc, queue/search/wake paths became prominent. They did not justify
writing a private freelist or removing the wake protocol's fence.

Sticky producer shards improved some four-worker noop samples but regressed an
eight-worker CPU payload by about 29%. Grouped shards and TLS producer
round-robin also failed to establish a broad win across detached/handled,
saturation, nested, uneven and idle workloads, including producer contention
and latency passes. These changes were removed. Central batch remains 1024;
an exhaustive batch/shard tuning campaign remains future work.

## Validation and local artifacts

- Release CTest: **43/43** passed, including shared completion observers,
  depth/Reject/error-policy checks, forward-iterator traversal and allocation
  failure rollback from a Reject-mode worker.
- Clang ASan + UBSan fuzz corpus replay: **19/19** layers passed. LeakSanitizer
  was disabled because it fails under this environment's ptrace restrictions;
  these runs do not establish leak coverage.
- TSan: **7/7** selected concurrency/API/scope/graph tests passed.
- Short mutation campaigns exercised wake protocol, saturation, pool and joins;
  the final wake-protocol campaign completed 3,296 executions. These are smoke
  campaigns, not exhaustive concurrency proofs.

Local scripts, raw samples, logs, source hashes and the tested source overlay
are preserved in `out/experiments/wait-ranges-v55cCh/` (git-ignored). The final
results are `retained-{probes,noop,latency}.jsonl`; `summarize-retained.py`
recomputes the table and intervals. `retained-measure.py`, `retained-latency.py`
and `probe.cpp` record the exact commands and probe implementation. Their paths
refer to the original build layout; see `manifest.json` for source/binary hashes
and build settings. Historical allocator/perf artifacts are in `audit/`.
Raw JSON and experimental patches are deliberately kept outside tracked docs.
