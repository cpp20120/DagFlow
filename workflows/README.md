# DagFlow workflows

Run these manifests with the standalone `CMakeWorkflow` executable from the
DagFlow source directory. CMake still owns native targets and incremental
rebuilding; `invoke` intentionally executes custom targets and campaigns.

```sh
CMakeWorkflow workflow plan workflows/tests-managed.json
CMakeWorkflow workflow run workflows/tests-managed.json --results-dir out/workflow-runs
CMakeWorkflow workflow report out/workflow-runs/<run-directory>
```

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

ASan and TSan have separate workflows (`validation-asan.json` and
`validation-tsan.json`) and separate CMake build trees. Benchmark smoke and
full campaigns likewise remain separate opt-in workflows.

## Additional test and build coverage

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
