# Measurement harness for `src/main.cpp`

`dagflow-example` is the stress harness. Everyday API examples live in
[`examples/`](../../examples/README.md). The separate
[runtime suite](runtime-suite.md) covers TaskScope/DAG reuse and cancellation.
This harness concentrates on Pool submission, queues, helping and waking.

## Build and run

```sh
cmake -S . -B out/harness -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE='-O3 -g -DNDEBUG' \
  -DDAGFLOW_BUILD_STATIC=ON -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_BUILD_TESTS=ON
cmake --build out/harness -j4
out/harness/dagflow-example --help
out/harness/dagflow-example --scenario external-contention \
  --workers 4 --producers 2 --tasks 131072 --iterations 0 \
  --warmup 3 --warmup-ms 500 --repeats 15 --min-ms 1000 \
  --verify checksum --json
ctest --test-dir out/harness --output-on-failure
```

Defaults: seven scenarios, 1 then 4 workers, **2 producers independent of worker
count**, persistent producers, 2 warmup runs and at least 200 ms of warmup.
`--warmup 0 --warmup-ms 0` explicitly requests a cold run. Pools, buffers and
expected values are constructed before warmup. Warmup executes the actual
workload and validates it; it is not an empty sleep.

| Scenario | Work and logical task count, with `N = --tasks` |
|---|---|
| external-contention | Multiple external producers; N tasks, native N=131072 |
| external-batch | Same payload/order/options; default batch=16, native N=131072 |
| hot-shard-skew | One external producer targets worker hint 0; N tasks, native N=65536, heterogeneous payload |
| local-overflow | One handled root spawns N detached children; N+1 tasks, native N=65536 |
| nested-helping | N detached roots each submit and wait for one handled child; 2N tasks, native N=16384 |
| mixed-chaos | N external roots, every eighth root spawns `fanout` children, every sixteenth submits/waits for another child; N+ceil(N/8)*fanout+ceil(N/16) tasks |
| idle-burst | One external producer submits N tasks after `idle-us`; native N=64 |

`local-overflow` with one worker and the default capacities spills 48,128 of
65,536 children to shared overflow. Diagnostics check `overflow_push` and
`overflow_acquire` against that count and `inline_execute == 0`. Historical
measurements before the [lifecycle change](../pool-lifecycle.md) executed these
children inline; their throughput is not the current queued-overflow throughput.

## Controls

| Option | Meaning |
|---|---|
| `--workers`, `--producers`, `--shards` | Independent counts; shards=0 uses runtime default (worker count) |
| `--tasks` | Root/external task count; default depends on scenario |
| `--iterations` | Override deterministic CPU payload iterations for every task; 0 measures a minimal payload |
| `--submit-batch 0..64` | External scalar/batch publication; external-batch maps 0 to its default 16 |
| `--central-batch 1..1024` | Runtime ingress drain size; **not** payload grain |
| `--fanout 0..64` | Number of local children in mixed-chaos; default 4 |
| `--high-every N` | Every Nth multi-producer root has high priority; 0 disables, default 8 |
| `--placement spread\|hot\|none` | Worker hints; hot-shard-skew always uses hint 0 |
| `--producer-mode persistent\|fresh` | Reuse external producer threads, or include their creation/join in each sample |
| `--repeats N`, `--bursts N` | Minimum measured repetitions; bursts applies to idle-burst |
| `--min-ms N` | Minimum **sum of timed sample durations**, excluding idle sleeps, reset and validation |
| `--max-samples N` | Bounds repetitions; JSON `duration_satisfied=false` signals a short run |
| `--idle-us N` | Delay before each measured idle burst (default 10000); does not guarantee workers actually slept |
| `--verify checksum\|exact\|off` | Payload validation mode, described below |
| `--latency --sample-stride N` | Sample submit/start/finish, default stride=64; idle-burst samples every task |
| `--cpus LIST` | Restrict all threads to a subset of the inherited allowed mask |
| `--worker-cpus LIST`, `--producer-cpus LIST` | Separate worker and controller/producer masks; comma/range syntax |
| `--json` | One JSON object per scenario/worker configuration, including raw samples |

Only external-contention, external-batch and mixed-chaos use `--producers`.
Other scenarios use the controller as their sole external producer. Local,
nested and mixed publication is scalar; their effective `submit_batch` is 0
in the result even if a batch option was supplied.

Workers inherit a CPU **group mask**; this does not bind each worker to a unique
core. Persistent/fresh producers bind individually round-robin within their
mask. The controller shares the producer mask. Affinity hints select a runtime
shard and do not guarantee the worker that ultimately executes the task.

External submission plans group tasks by affinity and priority *before timing*,
so scalar/batch comparisons use identical per-producer ordering. This differs
from the historical main's interleaved order. Old and new reports also differ
in producer lifetime, verification and task representation: do not attribute
their timing difference solely to one runtime change.

## Verification and measurement boundaries

Each logical task has a predetermined seed, payload and output slot. The
expected results are precomputed outside timing. Checksum mode checks every
slot's result and visit marker after drain; `exact` uses an atomic visit count
to detect duplicates too. `off` keeps a thread-local volatile sink to prevent
payload removal but does not validate coverage. A child submission/wait failure
is recorded and fails the harness even for detached roots.

Verification stores/atomics and timestamp collection execute inside tasks, so
they have a measurable cost. Use the same mode for comparisons. For very small
tasks, measure `--verify off` separately and retain an exact-validation run.
The harness is a whole-workload measurement, not just scheduler instructions.

Every measured sample follows this order:

1. Reset drained buffers, then optional idle sleep.
2. Snapshot diagnostic counters and process resource usage.
3. Enable perf and wait for its ACK, when controlled by the runner.
4. Start the wall clock; release producers / submit work; wait for all work.
5. Stop the clock; disable perf and wait for its ACK.
6. Snapshot resource usage/counters, validate payloads, collect the sample.

Wall time includes producer coordination/publication and drain. It excludes
setup, reset, sleeps and verification scans. Perf and `getrusage` windows are
slightly wider and include phase-control overhead. Idle-burst results can be
dominated by this overhead for counters; use wall time/latencies for latency
analysis. Perf control timeout fails the run instead of silently collecting
startup/warmup. Both newline-only and NUL-terminated ACKs are supported.

JSON schema 2 retains elapsed/user/system times, voluntary/involuntary switches,
minor/major page faults and process peak RSS (Linux KiB). Peak RSS is a process
high-water mark, not a per-sample delta. It also retains per-sample diagnostic
counts and sampled submit-to-start/submit-to-finish vectors. Latency entries are
ordered by logical slot, including roots and children. Parent finish includes
its spawning/waiting body; it precedes callable destruction/completion epilogue.
`first_start_us` and `last_finish_us` describe the sampled subset. Consequently
`drain_tail_us` with stride>1 also includes any unsampled work after the last
sampled finish; it is not an exact scheduler teardown duration.
Percentiles describe the recorded samples, without extrapolating tails: seven
repeats or 64 bursts are not enough to establish a stable p99. Increase the
sample count for tail analysis and inspect the raw distribution across processes.

Workload storage remains alive until `wait_idle()`, including exceptional partial
publication. Persistent producers are joined before that storage is released.

## Reproducible Release / LTO matrix

```sh
python3 scripts/benchmark_main.py --out out/profiles/main-harness \
  --workers 1,4 --producers 2 --rounds 3 --repeats 9 \
  --warmup-ms 500 --min-ms 500 --profile-ms 1000
```

The output directory must be new. The runner builds a frozen source snapshot
with `-O3 -g -DNDEBUG`, static linking of DagFlow, no LTO and full LTO. The
allocator defaults to mimalloc; `--allocator system|tbbmalloc` changes it.
External allocator libraries remain system dependencies, not frozen sources.
The manifest records source/binary hashes, compiler output, commands, allowed
CPU topology, governor/frequency snapshot and exact case settings. It never
changes system governors or disables turbo.

Default affinity takes one logical CPU per physical core and assigns disjoint
worker/producer groups. All configurations share the same producer mask. If
there are insufficient cores, explicitly choose `--affinity inherit` for an
oversubscription experiment. Hybrid core type, NUMA placement, thermal state and
other host load still need consideration; physical-core selection is not CPU
isolation and recorded frequency is just a snapshot.

Timing order is shuffled with a recorded seed. Each process creates a new pool
and warms it. The table reports the median of process medians and their range;
all individual runs remain available. Timing, perf stat, diagnostics and stack
sampling run separately. The runner fails if the requested minimum duration
was not reached. Counter runs use fixed repeat counts and normalize by the
actual logical task count times sample count.
For idle-burst the runner uses a fixed `--bursts` count (default 64) and no
active-duration target. Extending microsecond bursts to hundreds of active
milliseconds would otherwise spend minutes in inter-burst sleeps. Use the
standalone harness if that long idle experiment is intentional.

Default events:

- `cycles:u`, `instructions:u`, `branches:u`, `branch-misses:u`;
- `cache-references:u`, `cache-misses:u` in a separate pass;
- task-clock, context switches, CPU migrations, page faults, minor/major faults.

Hardware events count user space. Availability is probed; unavailable events
are reported, never replaced by zero. Raw perf CSV retains running percentages
and statuses; multiplexing below 90% is flagged. Counters from different passes
must not be treated as a single synchronized observation. Optional events can
be requested with `--extra-events L1-dcache-load-misses,dTLB-load-misses`; each
additional event gets its own measured pass. Support is machine dependent.

`--perf all` also produces cycles-weighted DWARF flamegraphs from the exact
baseline binaries, text callgraphs, hotspots and raw `perf.data`. Sampling has
its own warmed process and `--profile-ms` active duration. `--frequency` sets
the sample frequency (default 199 Hz). Short idle-burst runs instead
use `--idle-sample-period` cycles per sample (default 10000); their independent
recording has higher relative overhead. Unresolved/truncated stacks and lost
sample warnings remain visible in artifacts/logs; increase duration before
drawing conclusions from a sparse graph. DWARF stack capture has overhead and
its elapsed time is excluded from the timing table. If perf lacks Python
scripting support, raw data and text callgraphs remain available with a warning.

Artifacts: `report.md`, `summary.csv/json`, `manifest.json`, `commands.json`,
per-run JSON, raw perf CSV, `stacks-*/flamegraph.svg`, callgraphs and logs.
`--perf stat|off` and `--no-diagnostics` reduce collection cost. Every subprocess
has a timeout; on timeout its process group is terminated. Partial logs and
manifest are kept for diagnosis.

Use matrix options (`--shards 0,1`, `--batches 0,16,64`,
`--iterations native,0,32`) or `--cases path.json` for a focused experiment:

```json
[
  {"scenario":"external-contention", "workers":4, "producers":4, "shards":1, "submit_batch":0, "iterations":0},
  {"scenario":"external-contention", "workers":4, "producers":4, "shards":1, "submit_batch":16, "iterations":0},
  {"scenario":"external-contention", "workers":4, "producers":4, "shards":4, "submit_batch":16, "iterations":0},
  {"scenario":"local-overflow", "workers":1, "tasks":65536},
  {"scenario":"nested-helping", "workers":4, "tasks":16384},
  {"scenario":"idle-burst", "workers":4, "tasks":64, "idle_us":10000}
]
```

Avoid unnecessarily multiplying every knob: many are irrelevant to some
scenarios. Per-case JSON can also set producer lifetime, central batch, fanout,
priority frequency, placement and CPU masks.

## Runtime diagnostics

`-DDAGFLOW_RUNTIME_DIAGNOSTICS=ON` enables 27 path counters: publication source,
inline overflow, queue ingress/full/retry, local/drain/remote acquisition,
steal probes/outcomes, wake calls/signals, parking/timeouts, helping, completion
allocation, batch publication, execution and cross-thread packet free.

Snapshots are atomic, process-wide and taken around drained work. Probe/park
events may change at the boundaries because idle workers continue running.
`wake_signal` means a waiter was claimed/signalled, not proof of a kernel wake
or actual sleep. `cross_thread_free` compares allocating/executing thread IDs;
it does not measure allocator remote-free internals. `packets` counts prepared
tasks entering publication, not total allocator calls or bytes.

The first 4096 participating threads get private counter lanes. Later threads
share an overflow lane; counts remain exact, but contention changes. JSON
reports overflow (relevant to long `fresh` experiments). Thread counters remain
available after thread exit. Diagnostics add atomics and change packet layout;
**never use that build's elapsed time as the baseline**. Normal builds compile
the hooks to no-ops. The CMake macro is public to keep packet ABI consistent.

## Callable representation

Current Pool packets already use `ScheduledTaskModel<Callable>`: one allocation
contains the common header and concrete lambda. A static ops table supplies
invoke/destroy for the heterogeneous pointer queues. There is no `small_function`
inside those packets and no separate callable spill allocation. The wrapper
lambda and `std::invoke` can inline inside the concrete invoke thunk; the worker
still dispatches indirectly to that thunk and to typed destruction.

Reusable graph node work still uses `small_function`, with different ownership
and reuse requirements. Removing that separately requires preserving graph
lifetime and concurrent-run semantics. The harness's `std::function` calls a
producer loop once per burst; it is not used to dispatch every runtime task.
An old/new representation performance claim needs paired builds with identical
harness, payload, allocator, CPU masks and validation mode. This suite alone
does not establish that claim.
