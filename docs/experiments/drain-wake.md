# Drain and acquisition-side wake experiments

Scope: ingress drain and acquisition-side wake only. Allocators, graph execution,
task representation, parking memory orders and submission policy are unchanged.
Sources and raw runs are under `out/experiments/drain-wake/`.

## Findings

The original drain executes a scalar MPMC pop for each task and publishes every
sibling separately into the local Chase–Lev deque. Every successful Pool
acquisition then checks `has_local_work()` and calls `wake_one()` if it is true.
That includes ordinary local pops that do not publish any work. On the measured
external stream this means approximately two fenced wake calls per task.

Four initial variants separate the changes:

- `baseline`: original drain and wake-after-local-pop policy.
- `drain`: up to 64 MPMC elements claimed with one head CAS, then one local
  bottom release for a group. Total drain limit remains `central_batch`.
- `wake`: scalar drain; acquisition reports whether it published siblings.
- `combined`: both changes.

The bulk implementation passes the queue and ownership tests but regresses
external throughput substantially. In the first O3 comparison (four workers),
external-contention takes 4.15 ms in baseline, 7.73 ms with bulk drain and
14.58 ms with both changes. External-batch takes 2.77, 4.41 and 4.67 ms,
respectively. These are medians of five independent process medians, not a
single fastest run. See `o3-first/report.md` for all configurations and ranges.

A separate uninstrumented perf stat pass of external-contention gives roughly
702 user cycles/task and 614 instructions/task in baseline versus 1225 and 830
with bulk drain. Diagnostic builds show a large change in execution pattern:

| Path, per logical task | Baseline | Bulk drain | Wake only |
|---|---:|---:|---:|
| wake calls | 1.9961 | 1.3800 | 1.0037 |
| wake signals | 0.0002 | 0.1212 | 0.0001 |
| parking calls | 0.0001 | 0.0540 | 0.0001 |
| steal probes | 0.0117 | 1.8856 | 0.0083 |

This supports increased parking/waking and stealing as contributors to the
regression; it does not prove a unique cause. Instrumentation changes execution
timing, so diagnostic counts must not be substituted for baseline timings.
`wake_signal` means a claimed notification, not necessarily a kernel wake.

Two follow-ups were also rejected:

- `local_batch` / `local_batch_wake`: preserve scalar MPMC pops and only batch
  the local publication. Results are mixed, including throughput regressions;
  `local-batch/report.md` retains them.
- `drain_retry` / `drain_retry_wake`: recheck ingress after a partially filled
  batch instead of ending drain immediately. This does not remove the external
  regression, including with full LTO (`retry-lto/report.md`).

The bulk APIs and their scratch buffers were removed from production queues.
Experiment patches and the additional bulk queue tests are archived alongside
this document. There is no runtime switch retaining the rejected alternatives.

## Retained wake protocol

The initial `wake` and `wake_once` candidates are **not retained**. An eight-worker
recruitment test exposed a missing relay: a worker can steal the last task from
one deque while another deque still has work. Waking only when the current
worker stages siblings can stop recruitment before all sleepers find work.
The original baseline also fails the independent eight-worker reproducer
(`drain-wake-data/recruitment.cpp`), waiting for the parking timeout. This is
not merely a regression introduced by the experiment.

The retained variant `relay` uses the original scalar drain. A `bool&`
acquisition result reports whether work came from ingress or another worker:

```text
ordinary local pop                     → consume covered local work → no wake
successful ingress drain or probe      → relay recruitment → wake
successful steal, including one task   → relay recruitment → wake
```

The result is reset at acquisition entry and set on the successful nonlocal
return path, not on every transferred element. No shared flag, epoch, cache,
timer or new atomic is introduced. Submission still performs its existing wake
protocol. The ordinary local-pop path avoids both `has_local_work()` and the
fenced wake call.

Why not remove acquisition-side wake entirely? Transfer has a visibility gap:
another worker can complete its final scan after the old queue releases a task
but before the new local queue publishes it. Drain and steal must therefore
finish publication and run the existing fenced wake handshake before entering
user code, including code that may help/wait for a sibling.

Why not wake after every local pop? A local pop consumes work already covered
by its submit/transfer publication. It does not create another visibility gap.
Successful thieves and ingress consumers continue recruiting sleeping workers,
including when they publish no local siblings. This extra case is necessary:
emptiness of the selected source is not emptiness of all queues. An owner that
found work after announcing parking must cancel
its own announcement before waking another worker; otherwise it could claim its
own idle bit. This ordering remains unchanged.

The handshake in `ParkingLot::prepare/wait/wake_one`, its fences, epoch and
mutex ordering are unchanged. This optimization does not replace publication
ordering with a relaxed idle probe or a heuristic `need_wake` flag.

## Final baseline / relay comparison

The final matrix contains 380 timing processes: 19 cases, two variants, O3 with
and without full LTO, and five independent rounds. Warmup and minimum measured
active time are each 100 ms (idle-burst uses its explicit burst/sleep schedule).
Separate diagnostic builds report these calls per logical task with four workers:

| Scenario | O3 baseline | O3 relay | LTO baseline | LTO relay |
|---|---:|---:|---:|---:|
| external-contention | 1.99568 | 1.03471 | 1.99640 | 1.03450 |
| external-batch | 1.06016 | 0.09578 | 1.06057 | 0.09584 |

This removes approximately 48% and 91% of wake calls, respectively. It does
**not** produce a consistent wall-time improvement. Default four-worker external
submission changes by -0.2% without LTO but **+9.2% with LTO**; batch submission
changes by +1.4% and approximately 0%. Four-worker nested-helping regresses 5.0%
without LTO. Other cases improve: idle-burst changes by -5.4% / -11.7%, and
external-contention with central_batch=32 by -15.6% / -8.5%. These are observed
median changes, with overlapping ranges in several cases, not guarantees.

The reason to retain this protocol is the reproduced recruitment fix and fewer
redundant wake calls, not a claim that every workload becomes faster. Bulk drain
is rejected; scalar drain and both queue implementations remain unchanged.

The [complete table](drain-wake-data/final-relay-report.md) includes the range of
process medians. [Summary CSV](drain-wake-data/final-relay-summary.csv),
[run manifest](drain-wake-data/final-relay-manifest.json), and
[raw perf CSV contents](drain-wake-data/final-relay-counters.json) preserve the
results. The raw counter archive maps each original filename to its CSV text;
the full per-process harness and diagnostics JSON remains in
`out/experiments/drain-wake/final-relay/`.

## Validation and methodology

Regression coverage includes acquisition publication flags, transfer followed
by a notified wait, and actual Pool recruitment of 2/4/8 sleeping workers from
one external batch wake. The latter runs both priorities with a five-second
parking timeout and requires completion within one second, so correctness
cannot depend on that timeout. Existing fairness, nested waits, overflow,
backpressure and partial-publication tests also apply.

Final validation: Release 28/28 tests, ASan/UBSan with leak checking 28/28 tests,
and TSan queue, scheduler-topology and pool-queue suites 3/3. The sanitizer builds
use the system allocator. The standalone eight-worker reproducer fails baseline
at round zero and passes all 20 rounds with relay; its
[recorded output](drain-wake-data/recruitment-results.json) and
[source](drain-wake-data/recruitment.cpp) are archived. The
[retained patch](drain-wake-data/retained-relay.patch) includes runtime changes
and regression tests relative to the frozen baseline.

Rejected bulk code additionally received exact-once payload checks under
concurrent scalar/batch consumers, ring wraparound, deque thieves, capacity
boundaries and a deliberately unpublished ring head. Passing these tests was
not a reason to retain a performance regression.

Each experiment uses frozen sources and identical `main.cpp` hashes across
variants. Builds use Clang, `-O3 -g -DNDEBUG`, static DagFlow and mimalloc, with
no LTO or full LTO as labeled. Physical CPU masks are disjoint for workers and
producers. Four-producer cases intentionally share two producer CPUs, held
fixed across variants. There are 19 cases: seven main scenarios at 1/4 workers,
central-batch=32 external cases, nontrivial payloads, and shared-ingress
contention. Verification is `checksum`; timings and perf/diagnostics are
separate runs. Process order is shuffled with a recorded seed.

The matrix includes substantial variance in some short workloads and idle
latency. Changes of a few percent are not a universal speedup claim. Consult
the range of process medians as well as the aggregate. Perf hardware counts
are user-space counts. This host reports software events with `:u`; those are
not full kernel-inclusive context-switch totals. Process `getrusage` samples
also retain user/system CPU time and voluntary/involuntary switches.

To rerun saved snapshots into a new results directory:

```sh
python3 scripts/benchmark_drain_wake.py out/experiments/drain-wake \
  --name another-run --variants baseline,relay --profiles o3,o3-lto \
  --rounds 5 --warmup-ms 100 --min-ms 100 --diagnostics --counters
```

Each result directory contains commands/logs, source and binary hashes, raw
harness JSON, summary CSV/JSON/Markdown and optional diagnostic JSON / perf CSV.
The runner expects `sources/<variant>` snapshots; it does not patch or reset the
working tree. Archived patches describe changes relative to the recorded
baseline, which already contains the typed-task and harness work.
