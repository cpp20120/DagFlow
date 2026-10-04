# Runtime benchmark suite

`bench/runtime_suite.cpp` supplies checked workloads; `scripts/benchmark_suite.py`
builds and runs the matrix sequentially and saves JSONL/CSV results and provenance.
The earlier `dagflow-runtime-bench` executable and historical reports remain
available. Their workloads/timing boundaries differ; do not merge those numbers
with this suite as though they were the same benchmark.

The [STL allocator routing experiment](stl-allocator-routing.md) uses this suite
for paired before/after runs across all three allocation backends. Its dedicated
runner preserves source snapshots and changes only the runtime STL allocator uses.

The [main.cpp O3/Full LTO profile](main-o3-lto.md) is a separate experiment using
the stress workloads in `src/main.cpp`, with perf counters and flamegraphs.
Its whole-process counters and timing boundaries differ from this suite.

## Quick start

Python 3, CMake, Ninja, Clang and lld are required for the build matrix. PGO also
requires a matching `llvm-profdata`. The selected allocator must be installed;
`system` has no allocator dependency.

```sh
python3 scripts/benchmark_suite.py \
  --profiles release --allocator system \
  --workers 1,2 --work-ns 0,1000 --tasks 512 --repeats 3 --warmup 1
```

The complete build matrix has independent Release, ThinLTO, PGO, and ThinLTO+PGO
variants. All use the same static linkage and common compiler options; native CPU
flags are not added. Choose worker counts compatible with your allowed CPU set.

```sh
python3 scripts/benchmark_suite.py \
  --profiles release,lto,pgo,lto-pgo --allocator mimalloc \
  --workers 1,2,4,8 --work-ns 0,100,1000,10000 \
  --tasks 16384 --repeats 40 --warmup 5 --sample-stride 4 \
  --output out/benchmarks/full-matrix
```

Output directories must be new: the runner refuses to overwrite measurements.
Repeat with different `--allocator` values to compare backends. Builds and timed
runs are sequential; matrix order is shuffled with a fixed seed (1729) to spread
order effects. Compiler/build logs and the actual order are retained.

For an existing binary, including a non-Clang build:

```sh
cmake -S . -B out/build/suite -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DDAGFLOW_ALLOCATOR=system \
  -DDAGFLOW_BUILD_RUNTIME_BENCH=ON -DDAGFLOW_BUILD_EXAMPLES=OFF
cmake --build out/build/suite --target dagflow_runtime_suite -j 4
python3 scripts/benchmark_suite.py \
  --binary out/build/suite/dagflow-runtime-suite \
  --workers 1,2,4 --scenarios scope_recursive,graph_reuse
```

`--binary` skips building/training, labels the profile `existing`, and hashes the
binary. The source manifest then describes the current tree, not a verified
source-to-binary relationship. Preserve the original build cache/flags yourself.

## Scenarios and boundaries

All scenarios check every payload result after each repetition, outside its timed
region. Output slots belong to individual tasks; there is no shared completion
increment in the ordinary payload. Slot writes and scheduler accounting are still
part of execution cost, so zero-work results are not pure scheduler instruction
counts. Pool startup, expected-result calculation, slot reset and warmup are
excluded. Completion and `wait_idle()` packet cleanup are included before slots
are reused. Submission errors fail the run; accepted tasks drain before their
borrowed benchmark storage is destroyed.

| Scenario | Work and measured boundary |
| --- | --- |
| `local_saturated` | A worker publishes a wide batch of detached children. With one worker and more than local capacity this forces overflow into ingress; other workers may steal fast enough to avoid saturation |
| `steal_heavy` | A producer worker submits batches of at most 256 children and remains occupied until thieves execute them. Includes a per-child atomic counter and producer yielding; explicitly skipped at one worker |
| `external_detached` | One external producer submits a batch without individual handles |
| `external_handles` | Same external batch with individual completion handles and waits; handle vector capacity is reserved before timing |
| `idle_burst` | Wait for the pool to drain, sleep `--idle-us` outside timing, then externally submit a burst. Measures request/start latency after idle, not an isolated OS wake syscall; sleep does not prove every worker parked |
| `nested_spawn` | A binary tree of pool submissions; each parent cooperatively waits for its children |
| `scope_recursive` | Binary-tree descendants through `TaskScope::Context`; scope creation, admission close, join and destruction are timed |
| `fanout_fanin` | Prebuilt/sealed root → N leaves → join DAG; reuses the graph between repetitions |
| `deep_dag` | Prebuilt/sealed N-node chain, including bypass behavior; reuses the graph |
| `uneven` | Wide local batch where every sixteenth payload performs 32 times the base iterations |
| `graph_reuse` | Prebuilt/sealed N independent nodes, repeated runs without rebuilding topology |
| `dag_build_run` | Build, seal, execute and destroy an N-node chain inside every timed repetition |
| `graph_tokens_serial` | One prebuilt node with N tokens and concurrency=1; reuses the graph |
| `graph_tokens_parallel` | One prebuilt node with N tokens and concurrency limited by the pool; reuses the graph |

The token scenarios use one benchmark-owned relaxed atomic ticket per invocation
to select a unique output slot, because the public node callable takes no token
index. This ticket and the checked payload are part of their measured cost;
their timings are not pure token-claim overhead. The ticket is reset between
drained runs, and both count and per-slot outputs are verified.

With `DAGFLOW_RUNTIME_DIAGNOSTICS=ON`, JSON additionally contains `diagnostics`:
counter deltas summed over measured runs only, from just before the timed region
through its final `wait_idle()`. Divide by `repeats` for per-run counts. Ordinary
builds emit null and compile the counter snapshots away. These builds must be
timed separately: instrumentation changes task layout and performs atomic RMWs.
The memory counters measure successful `runtime_memory` allocation requests and
requested bytes, not backend rounding or retained/RSS bytes. `packet_allocations`
distinguishes owning packets from prepared graph-slot publications; `packets`
counts publication attempts through the pool, regardless of storage ownership.
Use [the graph allocation experiment](../experiments/graph-allocations.md) and
`scripts/measure_graph_allocations.py` for the focused comparison.

`tasks` is the number of **payload** invocations. Root submission tasks and the
empty fan-out/fan-in nodes are not included in that denominator. Every scenario
executes the same payload count, except for these explicitly uncounted control
nodes. The skew in `uneven` changes work per payload; compare like scenarios.
At zero iterations even the uneven payload has no arithmetic loop.

By default shards = workers; `--shards N` sets a fixed domain count independently
of worker count in both the executable and runner. Central batch defaults to 32;
the executable accepts `--central-batch N`. Pinning = false.
CPU affinity/cpuset restrictions inherited from the launcher still apply. The
manifest records allowed CPUs on systems exposing `sched_getaffinity` and captures
Linux CPU information. The suite does not change frequency governors or affinity.

The executable accepts `--submit-batch 0..64` for `external_detached` and
`idle_burst`. Zero preserves scalar submission; positive values use the explicit
[detached batch API](../batch-submit.md), including a final partial group. Other
scenarios retain scalar submission. JSON reports the effective `submit_batch`
and `central_batch`. Input-vector capacity is reserved outside timing; callable
construction and batch staging remain inside. Latency starts when each input
callable is created, including time spent preparing the rest of its batch.

`scripts/benchmark_batches.py --before OLD_BINARY --after NEW_BINARY --output NEW_DIR`
runs a focused Linux comparison of scalar submission in both binaries and batch
sizes 1/4/16/64. It checks matching payload checksums, retains raw results and
commands, and restricts each process to up to workers+1 distinct physical cores
from the allowed CPU set. Threads are not individually pinned. The matrix covers
1/4 workers, 0/400 iterations, and single-task latency with and without a preceding
idle interval. See [the results](batch-submit.md).

## Work sweep and measurement modes

The payload is a data-dependent integer arithmetic loop whose output is checked.
The runner calibrates nanoseconds per iteration once using its Release reference
(or the supplied binary), then passes **the same iteration count** to every
worker count and build variant at each requested `--work-ns` point. The requested
nanoseconds are approximate labels, not enforced durations; frequency and codegen
can change actual cost. Calibration does not add a clock read to each payload.
Use `--iterations` directly with the executable for exact reproducible loop work.

The runner defaults to separate `throughput` and `latency` invocations:

- Throughput has no per-payload clock reads. `payload_tasks_per_second` is total
  measured payload invocations divided by the sum of measured run durations.
- Latency timestamps every `--sample-stride`th payload. These reads/captures affect
  execution and callable size; compare latency-mode throughput only with the same
  mode. The full per-run durations are saved as `run_us`.
- For submissions, `latency_kind=submit_to_start` starts just before constructing
  the submitted wrapper. It includes allocation, queueing and any producer
  backpressure; it is not queue residence time alone.
- For DAGs, `latency_kind=run_to_start` starts immediately before `graph.run()`.
  It includes dependency delay; it is not node-ready-to-execution latency. Graph
  construction is excluded from this latency even in `dag_build_run`.

JSONL and CSV contain p50/p99/p999 for run duration and sampled task latency,
`latency_samples`, warmup/repeat counts, mode, work iterations and a checksum.
Quantiles use nearest rank. Small sample counts make p999 simply the maximum;
collect enough samples and independent runs before interpreting tail behavior.
Absent task-latency measurements are `null`, not zero. Skipped scenarios have a
reason and no invented throughput. The runner rejects checksum disagreement
between profiles, worker counts and measurement modes.

PGO trains each variant separately with 1024 payloads, 64 iterations, two measured
repetitions plus one warmup, and up to four requested workers, in both modes.
Training includes all scenarios and its results never enter the measured table.
Profiles are merged from a fresh training directory; training and PGO-use build
logs are preserved. PGO numbers describe this training distribution, not a promise
of improvement for unrelated workloads.

## perf / PMU

```sh
python3 scripts/benchmark_suite.py \
  --binary out/build/suite/dagflow-runtime-suite \
  --workers 4 --work-ns 1000 --scenarios scope_recursive,graph_reuse \
  --modes throughput --perf \
  --perf-events cycles,instructions,cache-misses,context-switches
```

`--perf` adds a separate `perf stat` invocation per successful throughput point.
Primary timings remain uninstrumented. Semicolon-separated raw counter files,
stderr and `perf-status.json` retain permission failures, unavailable events and
multiplexing information. A failed optional perf run does not discard primary
results or pretend counters were collected. Inspect event availability and
running percentages before comparing counters.

Counters cover the **whole process**, including pool startup, expected-result
calculation, validation, warmup and idle intervals. They are not scoped to just
the timed scheduler region and must not be interpreted as precise cycles/task.
Use large measured batches/repetition counts to reduce fixed overhead, or add
region-specific measurement for a focused experiment.

## Artifacts and validation

Each runner output contains:

- `results.jsonl`: structured results including all measured run durations;
- `results.csv`: flat table for analysis;
- `manifest.json`: options, calibration, source/binary hashes, platform, allowed
  CPUs, compiler for managed builds, git revision and dirty-tree status;
- `commands.json`, `logs/`: commands, exit codes, elapsed wall times, raw stdout
  and stderr, build/training logs and optional perf files;
- `build/`: managed builds and their CMake caches, PGO raw/merged profiles.

With `DAGFLOW_BUILD_TESTS=ON` and Python available, CTest registers
`dagflow_benchmark_suite_tests`. It checks one/multiple-worker execution, tiny and
multi-batch workloads, local overflow, checksums, JSON quantiles/sample counts,
CLI errors, matrix serialization and refusal to overwrite results. It uses no
performance thresholds.

The implementation was validated with a short 384-point matrix: four profiles,
1/2 workers, 0/~1000 ns payloads, 12 scenarios, two modes, 512 payloads and three
measured repetitions. There were 368 successful rows and 16 intentional one-worker
stealing skips. This validates the harness, not a performance ranking. The smoke
tests also ran under ASan/UBSan (LSan disabled in this environment) and TSan. A
separate `perf stat` smoke run collected cycles, instructions, cache misses and
context switches. Larger scaling and statistically useful tail studies remain
measurements to perform with this suite.
