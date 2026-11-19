# DagFlow workflows

Run these manifests with the standalone `CMakeWorkflow` executable from the
DagFlow source directory. CMake still owns native targets and incremental
rebuilding; `invoke` intentionally executes custom targets and campaigns.

```sh
CMakeWorkflow workflow plan workflows/tests-managed.json
CMakeWorkflow workflow run workflows/tests-managed.json --results-dir out/workflow-runs
CMakeWorkflow workflow report out/workflow-runs/<run-directory>
```

## GitHub Actions runtime regressions

[DagFlow runtime regressions](../.github/workflows/dagflow-runtime-regression.yml)
runs alongside the existing CI. Linux and Windows each have a benchmark job
and a contention job. The benchmark job runs `benchmarks-smoke.json` and
`validation-runtime-load.json`, which exercise the runtime and stress workload
families with bounded parameters. It checks that the paced, idle-burst and
priority metrics reach History. The contention job runs
`validation-contention.json` for scheduler, shutdown, backpressure and harness
schema tests.

The jobs build CMakeWorkflow from a pinned upstream commit and publish a
framework-dependent CLI for the runner's .NET SDK. They check workload
completion, task results, test assertions and metric ingestion. Timing data is
retained for inspection. Performance comparisons belong to the baseline/candidate
recipes on a controlled host. Every job uploads its CMakeWorkflow reports and
logs, including the History database for the benchmark job.

## Research campaigns

`dagflow-research-linux.json` adopts an existing `core` tree. Its mutation and
Relacy targets are **optional** CMake targets. Configure the tree explicitly
with the published tool path first; configure Relacy when running weak-memory
checks. The workflow does not silently configure or infer preset provenance.
The queue node pins `include/dagflow/detail/ring_mpmc.hpp` by SHA-256, retains
a source snapshot in its run directory and records the campaign report as a
declared output. If that header changes, update the expected hash only after
reviewing the mutation anchors and experiment assumptions. The engine checks
declared bytes; it does not interpret queue or Relacy semantics.

```sh
cmake --preset core \
  -DDAGFLOW_TOOLS_EXECUTABLE="$(command -v CMakeWorkflow)" \
  -DDAGFLOW_RELACY_INCLUDE_DIR="$PWD/out/weak-memory/relacy"
CMakeWorkflow workflow plan workflows/dagflow-research-linux.json
CMakeWorkflow workflow run workflows/dagflow-research-linux.json \
  --only research.queue-mutation --results-dir out/workflow-runs
CMakeWorkflow workflow run workflows/dagflow-research-linux.json \
  --only research.weak-memory --results-dir out/workflow-runs
```

For mutation only, omit `DAGFLOW_RELACY_INCLUDE_DIR`. The full protocol
mutation campaign has a documented surviving weak-memory mutant and currently
returns nonzero by design. Select individual nodes with `--only`. The adopted
context reports unverified preset provenance; a known source or configuration
conflict still fails. Direct C# commands allow a specific mutation suite or
Relacy protocol; see [campaigns](../docs/cmake-workflow-campaigns.md).

## Fuzzing and profiling

`validation-fuzz-smoke.json` builds `dagflow_fuzz_queues` and replays its CTest
seeds. `validation-fuzz-campaign.json` explicitly invokes the existing
`fuzz_dagflow_fuzz_queues` target for a bounded 60-second campaign. Neither
workflow generates or overwrites the checked-in seed corpus. Generate a layer
when needed with `CMakeWorkflow dagflow fuzz seed --layer ranges --source-dir .`.

`dagflow-profile-linux.json` and `dagflow-profile-windows.json` build real
benchmark executables and use native profiler backends. Captures require the
host profiler and its permissions. `profile-main-o3-lto-linux.json` compares
the existing `bench-o3` and `bench-o3-lto` presets for one bounded
`dagflow_stress_bench` scenario and explicitly requires the perf CPU backend.
The old one-off main O3/LTO experiment has
its own historical report and additional source snapshot, affinity and
frame-pointer analysis; the new workflow does not claim to reproduce those
measurements. Current profiling also supports
`CMakeWorkflow dagflow profile run`.

### Select fuzz campaigns by layer

`validation-fuzz-layers.json` exposes one `<layer>-campaign` node for each of
the 18 layers covered in addition to `queues`. It configures its own fuzz build
tree and invokes the existing native campaign targets. Select the layers with
`--only`. Budget about 18 minutes plus build time for the full manifest at
60 seconds per layer. The existing `validation-fuzz-campaign.json` runs
the queue campaign. The native targets write generated inputs and crash
artifacts under the build tree.

```sh
CMakeWorkflow workflow plan workflows/validation-fuzz-layers.json --only graph-campaign --only wake_protocol-campaign
CMakeWorkflow workflow run workflows/validation-fuzz-layers.json --only graph-campaign --only wake_protocol-campaign --results-dir out/workflow-runs/fuzz-layers
```

The `fuzz` preset uses Ninja and requires GNU-style Clang, libFuzzer and its
sanitizer runtimes. On Windows use WSL or a native toolchain that meets those
requirements. To retain a campaign report and corpus for triage, use the
[fuzz CLI](../fuzz/README.md).

## Compare baseline and candidate on Linux or Windows

Create a detached worktree at `out/worktrees/baseline` and use the current
worktree as the candidate. Replace `BASELINE_COMMIT` with the desired revision.
If that worktree already exists, check its revision with
`git -C out/worktrees/baseline rev-parse HEAD` and reuse it for that baseline.
Both revisions need the `bench-release` preset and the runtime suite
target. Run the preparation from the same compiler environment on each host.
The manifest configures two separate Release build trees with the same preset.
The generated recipes adopt those trees and build both benchmark executables
before measurements.

```sh
git worktree add --detach out/worktrees/baseline BASELINE_COMMIT
CMakeWorkflow workflow run workflows/benchmarks-prepare-pair.json --no-history --results-dir out/workflow-runs/pair-prepare
CMakeWorkflow workflow generate benchmark-matrix --recipe workflows/recipes/runtime-pair.recipe.json --output out/runtime-pair.json
CMakeWorkflow workflow plan out/runtime-pair.json
CMakeWorkflow workflow run out/runtime-pair.json --history-db out/runtime-pair.sqlite --results-dir out/workflow-runs/runtime-pair
CMakeWorkflow history query --db out/runtime-pair.sqlite --metric runtime_p50 --json
```

Use a CMakeWorkflow build that supports benchmark recipe `metrics`. The matrix
alternates both variants across seven process repetitions for eight scenarios
and one or four workers. It stores `runtime_p50` (µs) and
`runtime_throughput` (tasks/s) from each benchmark's JSON output. Query by
`--variant baseline` or `--variant candidate` and, for example,
`--scenario "graph_tokens_serial [0001]"` for one worker. The two series
belong to one History run. `history compare` takes two separate History run
IDs. To rebuild this catalog from retained reports, run
`CMakeWorkflow history import out/workflow-runs/runtime-pair --recursive --db out/runtime-pair.sqlite`.
Reuse the generated manifest for another run with the same recipe. Generation
requires a new output filename when the recipe changes.

### Check queue and scheduler contention

Prepare the baseline/candidate trees as above. The two contention recipes
alternate variants and check the results of each workload.

| Recipe | Cases | History metrics |
| --- | --- | --- |
| [Ingress contention](recipes/runtime-contention-ingress.recipe.json) | Scalar and batched external submissions, plus mixed work. Four workers, one or four producers, and one or four shards. | `ingress_run_p50`, `ingress_submission_rate`, `ingress_start_p99`, `ingress_finish_p99` |
| [Scheduler contention](recipes/runtime-contention-scheduler.recipe.json) | Forced stealing, a local queue under submission pressure, and uneven payloads with two or four workers. | `scheduler_run_p50`, `scheduler_throughput` |

Ingress p99 values measure submission-to-start and submission-to-finish
for sampled tasks. The ingress recipe enables latency sampling, which adds
measurement overhead. The scheduler recipe measures throughput without task
latency sampling. History keeps each combination in its own numbered series.

```sh
CMakeWorkflow workflow generate benchmark-matrix --recipe workflows/recipes/runtime-contention-ingress.recipe.json --output out/contention-ingress.json
CMakeWorkflow workflow run out/contention-ingress.json --history-db out/contention.sqlite --results-dir out/workflow-runs/contention-ingress
CMakeWorkflow workflow generate benchmark-matrix --recipe workflows/recipes/runtime-contention-scheduler.recipe.json --output out/contention-scheduler.json
CMakeWorkflow workflow run out/contention-scheduler.json --history-db out/contention.sqlite --results-dir out/workflow-runs/contention-scheduler
CMakeWorkflow history query --db out/contention.sqlite --metric ingress_start_p99 --json
```

Run [validation-contention.json](validation-contention.json) to check
scheduler contention, the targeted queue paths, the deque's last-slot race,
and parking epoch reuse with the `adversarial-tiny` preset. Select
`--only pressure.test` for pool close/shutdown and batch backpressure checks
with standard queue capacity. Select `--only stress.test` and
`--only runtime.test` for the benchmark CLI and result schema checks.

```sh
CMakeWorkflow workflow run workflows/validation-contention.json --results-dir out/workflow-runs/contention-protocol
```

### Sweep arrival rate, idle gaps, shard load and payload cost

These recipes use the shared benchmark build trees. Each validates the
executed work and saves its measurements to History.
The existing `benchmarks-smoke.json` also runs short paced-ingress and
batched idle-burst checks.

| Recipe | Sweep | History metrics |
| --- | --- | --- |
| [Paced ingress](recipes/runtime-paced-ingress.recipe.json) | 50k, 250k, 1M, 4M and 12M incoming tasks/s, one or four producers/shards, and Normal-only or mixed priority submissions | `paced_scheduled_start_p99`, `paced_producer_lag_p99`, `paced_submit_call_p99`, `paced_submission_rate` |
| [Idle burst](recipes/runtime-idle-burst.recipe.json) | Idle gaps of 100 µs, 2 ms and 10 ms, bursts of 1, 64 and 512 tasks, 1/2/4 workers and scalar or batch-16 submission | `burst_run_p50`, `burst_start_p99`, `burst_finish_p99` |
| [Hot shard](recipes/runtime-hot-shard.recipe.json) | One producer concentrates uneven payloads on shard 0 with two or four workers and one or four shards | `hot_shard_run_p50`, `hot_shard_start_p99`, `hot_shard_finish_p99` |
| [Payload cost](recipes/runtime-payload-sweep.recipe.json) | All 18 runtime-suite workloads with 0, 16 and 256 payload iterations at two and four workers | `payload_run_p50`, `payload_throughput` |
| [Priority latency](recipes/runtime-priority-latency.recipe.json) | One High for every eight submissions, with unlimited and paced scalar ingress | Separate submission-to-start and submission-to-finish p99 for High and Normal |

To bracket saturation on a different host, estimate the unrestricted
completion rate by dividing `logical_tasks` by `ingress_run_p50` in seconds
for the matching `external-contention` workload. Use
`ingress_submission_rate` to see how fast producers submit. The
scheduled-start p99 includes producer lag. The harness applies pacing to
scalar external submissions. The idle delay is outside the timed region.
Use the single-task burst to isolate first-task latency.

Priority latency uses the candidate build by default so it works with older
baselines. Add a baseline variant when that revision also supports
`--priority-latency`. The paired paced recipe already compares Normal-only
and mixed-priority ingress using metrics supported by both revisions.

```sh
CMakeWorkflow workflow generate benchmark-matrix --recipe workflows/recipes/runtime-paced-ingress.recipe.json --output out/paced-ingress.json
CMakeWorkflow workflow run out/paced-ingress.json --history-db out/contention.sqlite --results-dir out/workflow-runs/paced-ingress
CMakeWorkflow history query --db out/contention.sqlite --metric paced_scheduled_start_p99 --json
```

### Capture counters for the same pair

Both profile recipes use the same serial-token workload and preserve timing
and profiler results in the run directory.

| Host | Recipe | Backend |
| --- | --- | --- |
| Linux | [runtime-pair-perf-linux.recipe.json](recipes/runtime-pair-perf-linux.recipe.json) | `perf stat` user-space counters |
| Windows | [runtime-pair-etw-windows.recipe.json](recipes/runtime-pair-etw-windows.recipe.json) | ETW/WPR PMC sampling for `TotalCycles` and `InstructionRetired` |

```sh
CMakeWorkflow workflow generate profile --recipe workflows/recipes/runtime-pair-perf-linux.recipe.json --output out/runtime-pair-perf.json
CMakeWorkflow workflow run out/runtime-pair-perf.json --results-dir out/workflow-runs/runtime-pair-perf --history-db out/runtime-pair.sqlite
```

```powershell
CMakeWorkflow workflow generate profile --recipe workflows/recipes/runtime-pair-etw-windows.recipe.json --output out/runtime-pair-etw.json
CMakeWorkflow workflow run out/runtime-pair-etw.json --results-dir out/workflow-runs/runtime-pair-etw --history-db out/runtime-pair.sqlite
```

The Linux capture requires access to PMU events via `perf`. The Windows
capture requires an elevated session with Windows Performance Toolkit
(`wpr`, `tracerpt`) and the requested sources in `wpr -pmcsources`.
Use the benchmark matrix for uninstrumented timings and compare counters
within the same backend and host.

## Detect performance regressions and replay a case

[runtime-regression-evidence.json](runtime-regression-evidence.json) runs
three [fixed workloads](cases/) through the native DagFlow harness:
`serial-tokens`, `paced-ingress` and `idle-burst`. It snapshots the case
files and retains the harness reports and replay binaries in each workflow
run. Run it twice on the same quiet host with the same toolchain. Use a
scratch worktree so both revisions use the same manifest and build path.

From the DagFlow root, replace `BASELINE_COMMIT` and `CANDIDATE_COMMIT`
with revision names. Both revisions need the `runtime-campaign` preset and
support for the selected workloads.

```sh
git worktree add --detach out/worktrees/regression-evidence BASELINE_COMMIT
cmake -E make_directory out/workflow-builds/regression-evidence/.cmake/api/v1/query
cmake -E touch out/workflow-builds/regression-evidence/.cmake/api/v1/query/codemodel-v2
cmake --preset runtime-campaign -S out/worktrees/regression-evidence -B out/workflow-builds/regression-evidence "-DDAGFLOW_RUNTIME_CASES=$PWD/workflows/cases/regression-runtime.json" "-DDAGFLOW_STRESS_CASES=$PWD/workflows/cases/regression-stress.json" -DDAGFLOW_CAMPAIGN_GROUP= -DDAGFLOW_CAMPAIGN_ROUNDS=12 -DDAGFLOW_CAMPAIGN_AFFINITY=inherit
CMakeWorkflow workflow generate benchmark-matrix --recipe workflows/recipes/runtime-resources.recipe.json --output out/regression-evidence/resources.json
CMakeWorkflow workflow run workflows/runtime-regression-evidence.json --history-db out/regression-evidence/history.sqlite --results-dir out/regression-evidence/baseline
CMakeWorkflow workflow run out/regression-evidence/resources.json --history-db out/regression-evidence/history.sqlite --results-dir out/regression-evidence/resources-baseline
git -C out/worktrees/regression-evidence switch --detach CANDIDATE_COMMIT
CMakeWorkflow workflow run workflows/runtime-regression-evidence.json --history-db out/regression-evidence/history.sqlite --results-dir out/regression-evidence/candidate
CMakeWorkflow workflow run out/regression-evidence/resources.json --history-db out/regression-evidence/history.sqlite --results-dir out/regression-evidence/resources-candidate
CMakeWorkflow history runs --db out/regression-evidence/history.sqlite
```

The 12 rounds are separate process measurements for each case. Pass the
baseline and candidate History IDs to `history detect`. The example sets
a 5% minimum effect and treats increases in median run time as worse.
Read the result status from the report. To profile one case, pass the
original workflow run directories to `profile replay` and keep the scratch
build directory available as the original working directory. Use
`runtime.campaign` for `serial-tokens` and `stress.campaign` for the other
two cases. Replay needs permission for its profiling passes, including
scheduler tracing. If a later capture fails, `profile analyze REPLAY_RUN_DIR`
can analyze the captures already saved in the replay directory.

```sh
CMakeWorkflow history detect BASELINE_ID CANDIDATE_ID --metric run_p50_us --direction higher-worse --min-percent 5 --alpha 0.05 --db out/regression-evidence/history.sqlite --json
CMakeWorkflow profile replay BASELINE_RUN_DIR CANDIDATE_RUN_DIR --node stress.campaign --case paced-ingress --backend auto --results-dir out/regression-evidence/replay --history-db out/regression-evidence/history.sqlite
```

### Compare CPU and memory use

The [resource recipe](recipes/runtime-resources.recipe.json) above records
six processes each for graph reuse, graph construction plus execution, and
external submission. History exposes sampled `peak_rss_bytes` and
`cpu_time` where the runner can collect them, plus `resource_run_p50`.
Use the IDs of the two `resources.json` runs for descriptive comparisons.

```sh
CMakeWorkflow history compare RESOURCE_BASELINE_ID RESOURCE_CANDIDATE_ID --db out/regression-evidence/history.sqlite --json
CMakeWorkflow history query --run RESOURCE_RUN_ID --metric peak_rss_bytes --db out/regression-evidence/history.sqlite --json
CMakeWorkflow history query --run RESOURCE_RUN_ID --metric cpu_time --db out/regression-evidence/history.sqlite --json
```

### Retain fuzz evidence

[validation-fuzz-evidence.json](validation-fuzz-evidence.json) prepares an
isolated libFuzzer build and replays seeds for graph, ranges and
wake_protocol. Use `--only configure` when you only need the build tree
for a different layer. CMakeWorkflow's fuzz commands handle campaigns,
triage and corpus merge.

```sh
CMakeWorkflow workflow run workflows/validation-fuzz-evidence.json --results-dir out/workflow-runs/fuzz-evidence
CMakeWorkflow dagflow fuzz run --build-dir out/workflow-builds/fuzz-evidence --layer graph --duration 60 --results-dir out/fuzz-evidence
CMakeWorkflow dagflow fuzz triage --run-dir FUZZ_RUN_DIR
CMakeWorkflow dagflow fuzz merge --coverage --run-dir FUZZ_RUN_DIR
```

Use the actual directory printed by `dagflow fuzz run` for `FUZZ_RUN_DIR`.
Keep that directory with its report, corpus and crash artifacts. Merge
successful campaigns back into the build-tree corpus. See
[fuzzing by layer](../fuzz/README.md) for the corpus and sanitizer settings.

## Additional test and build coverage

ASan and TSan have separate workflows (`validation-asan.json` and
`validation-tsan.json`) and separate CMake build trees. Benchmark smoke and
full campaigns likewise remain separate opt-in workflows.

These schema v2 workflows use existing presets, CTest labels, and CMake
targets. Run one branch with `--only`; a full run includes every independent
branch in that manifest. `plan` only reads build metadata. It can report an
unconfigured adopted tree as invalid until that tree exists.

| Workflow | Select with `--only` | What it runs |
| --- | --- | --- |
| `validation-models.json` | `models.property`, `models.fault-injection` | `core` preset, `dagflow_model_property_tests`, property/PBT and fault-injection CTest labels |
| `validation-adversarial.json` | `schedule.test`, `adversarial.test` | `adversarial-tiny` preset, schedule-exploration and adversarial target groups plus their CTest labels |
| `validation-build-package.json` | `build-check.ctest`, `package.cpack` | `ci-debug` build checks; `distribution` static/shared libraries and CPack package target |
| `benchmarks-extended.json` | `stress.smoke`, `tbb.smoke`, `stress.campaign`, `tbb.campaign` | Bounded native benchmark smokes and existing stress/TBB harness campaigns |
| `benchmarks-github-adopt.json` | `github.smoke` | Adopt an external GitHub snapshot build tree and run a bounded comparison smoke |
| `validation-variants.json` | `thinlto.smoke`, `fulllto.smoke`, `mimalloc.test`, `tbbmalloc.test` | LTO runtime suite smoke and allocator unit tests |
| `validation-pgo.json` | `pgo.use.smoke`, `lto-pgo.use.smoke` | Clang instrumented training/merge, declared profile artifact, profile-use build and bounded run |
| `validation-cross-build.json` | `linux-arm64.build`, `windows-x64.build`, `android-arm64.build`, `wasm32.build` | Cross compile `DagFlow_static`; no attempt to execute foreign binaries |
| `validation-docs-adopt.json` | `docs.generate` | Adopt an existing docs-enabled tree and invoke Doxygen target |

For example:

```sh
CMakeWorkflow workflow plan workflows/validation-models.json --only models.fault-injection
CMakeWorkflow workflow run workflows/validation-models.json \
  --only models.fault-injection --results-dir out/workflow-runs
CMakeWorkflow workflow plan workflows/validation-adversarial.json --only schedule.test
CMakeWorkflow workflow run workflows/validation-adversarial.json \
  --only schedule.test --results-dir out/workflow-runs
```

The benchmark campaign targets preserve their CMake harness arguments and
measurement files. Their workflow node durations are orchestration timings,
not benchmark metrics. The TBB branch requires TBB development packages.
Select `stress.smoke` or `tbb.smoke` for a quick check of the executable without
starting a full campaign. Those bounded arguments match the existing CTest
smoke recipes.
The GitHub branch requires an existing `bench-github` tree configured with
`DAGFLOW_GITHUB_SOURCE_DIR` pointing at a compatible external DagFlow source
snapshot. It is explicitly adopted, so its preset identity is unverified.
The adopted comparison is separate so the ordinary extended benchmark plan
does not depend on that external tree. The campaigns are opt-in and can run
for a long time.

```sh
mkdir -p out/build/bench-github/.cmake/api/v1/query
touch out/build/bench-github/.cmake/api/v1/query/codemodel-v2
cmake --preset bench-github -DDAGFLOW_GITHUB_SOURCE_DIR=/path/to/snapshot
CMakeWorkflow workflow run workflows/benchmarks-github-adopt.json \
  --only github.smoke --results-dir out/workflow-runs
```

The PGO branches require Clang and `llvm-profdata`, and require their build
trees to be configured with the same Clang toolchain from the first configure.
The `dagflow_pgo_merge` target intentionally performs training and writes the
profile named by the corresponding preset. The workflow verifies that profile
as an artifact before configuring the consuming tree. Artifact identity does
not attest to the complete compiler environment or profile quality. LTO and
allocator variants require their respective linker and allocator packages.
Cross builds require the toolchains/SDKs named by their presets; their
successful compilation would not imply runtime validation on the target OS.

The docs target is optional and has no dedicated configure preset. Prepare
its tree explicitly, then adopt it:

```sh
mkdir -p out/build/docs/.cmake/api/v1/query
touch out/build/docs/.cmake/api/v1/query/codemodel-v2
cmake -S . -B out/build/docs -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DDAGFLOW_BUILD_DOCS=ON
CMakeWorkflow workflow plan workflows/validation-docs-adopt.json
CMakeWorkflow workflow run workflows/validation-docs-adopt.json \
  --results-dir out/workflow-runs
```

This requires Doxygen. Existing tree adoption never runs configure. The
`docs` target is invoked intentionally; the `build` operation is reserved for
native targets.

## Python migration status

The current CMake and workflow entry points use CMakeWorkflow rather than
Python drivers. C# replaces the queue/protocol mutation campaigns, fuzz
seeds, and Relacy source-derived adapters/cases/campaign; the pinned historical
Relacy catalog is retained in the C# tests for byte-for-byte parity checks.

The four historical profiling drivers were replaced by
`dagflow profile run-historical`, `render-historical` and `flamegraph`.
The complete C# command path and archived numeric summaries were tested, but
this host denies the required PMU capture. A real O3/LTO capture remains
unverified; the old result files remain in `docs/benchmarks/main-o3-lto-data/`.
