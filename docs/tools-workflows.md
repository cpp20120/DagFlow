# DagFlow.Tools workflows

DagFlow.Tools runs explicit local work against an existing CMake build tree. It
is useful for chaining configuration, selected targets, tests, benchmark
harnesses and executable runs without replacing CMake's native build graph.
The supported workflow contract is schema version 2; version 1 remains accepted
for existing manifests.

## Ownership boundary

CMake and its generator own native target dependencies, compiler and linker
settings, presets, generated outputs, custom commands, incremental builds and
CTest definitions. DagFlow.Tools owns only workflow dependencies, requested
context identity, read-only observations, explicit CMake requests, scheduling,
cooperative ownership between DagFlow.Tools processes, execution records and
artifact validation. Every explicit `build` reaches `cmake --build`.

DagFlow.Tools does not reconstruct native dependencies, infer freshness from
files or codemodel metadata, run an implicit configure/clean/retry, or infer a
preset from cache values. A successful build request means that CMake's backend
returned success for that target and configuration. It does not attest all
inputs or the full environment.

## Context lifecycle

Contexts have three modes:

- `observe` allows the read-only `inspect` node and `context inspect` command.
  Planning and inspection do not create build trees or File API queries.
- `adopt` explicitly accepts an existing tree without DagFlow.Tools configure
  provenance. It never configures implicitly. Known source, generator or
  configuration conflicts fail; missing preset identity remains unverified.
- `managed` requires an explicit `configure` node before build, invoke, run or
  CTest operations. CMake interprets the named preset. The exact invocation and
  post-configure observation are journaled; later nodes reuse that session.

`provenancePolicy` is `strict` by default. If the File API generation changes,
DagFlow.Tools rereads and validates the current observation. Generation change
alone is not a configuration mismatch. Strict mode stops when the required
identity cannot be confirmed; `allowUnverified` permits continuation after
known conflicts have been excluded and records downgraded provenance. Neither
mode claims configuration freshness.

Observed context, requested context, configure invocation, generation
observation, native build verification, materialized artifact and experiment
result are separate facts in the journal. A `run` must depend on a successful
`build` node for the same executable and configuration. A raw `run.from` cannot
refer to `invoke`.

DagFlow.Tools leases serialize cooperating tools that use the same physical
build tree, including path aliases. Ordinary IDE or CMake processes do not
participate in these leases. Nested calls back into the same tree are rejected
to prevent build backend deadlocks. Cancellation stops launched process trees
where the platform provides that facility, records cancellation and blocks
dependents.

## Node kinds

| Kind | Meaning |
| --- | --- |
| `inspect` | Read-only build context observation. |
| `configure` | Explicit CMake configure request, optionally with a preset. |
| `build` | Request a known native target from CMake; a known UTILITY target is rejected. |
| `invoke` | Intentionally invoke any CMake target, including custom campaigns. |
| `run` | Execute an artifact from a successful `build` node. |
| `ctest` | Run CTest selection; the workflow must declare a build or invoke ancestor. |

Workflow `dependsOn` edges schedule these operations only. They do not replace or
modify CMake target dependencies. Resource names serialize nodes that claim the
same resource; `exclusive: true` waits for all active nodes and blocks new ones.

Schema v2 keeps build settings in CMake presets. A manifest contains named
contexts and ordered workflow nodes; a context names a build directory and may
declare its source directory, requested configuration, literal `configurePreset`,
observed generator expectation, lifecycle `mode` and provenance policy. Nodes
carry `id`, `kind`, `context` or a `run.from` build-node reference, optional
`dependsOn`, resource/exclusive constraints and a timeout. Run arguments,
working directory and environment belong only to executable run nodes. Configure
arguments are not mirrored in workflow JSON.

## CLI

Build the managed tool with .NET 10:

```sh
dotnet build tools/DagFlow.Tools/DagFlow.Tools.csproj -c Release
dotnet tools/DagFlow.Tools/bin/Release/net10.0/DagFlow.Tools.dll workflow plan workflows/tests-managed.json
dotnet tools/DagFlow.Tools/bin/Release/net10.0/DagFlow.Tools.dll workflow run workflows/tests-managed.json --results-dir out/workflow-results
dotnet tools/DagFlow.Tools/bin/Release/net10.0/DagFlow.Tools.dll workflow report out/workflow-results/<run-directory>
```

`workflow plan` validates the selected DAG and reports observed and unknown
state without invoking CMake, creating results, or taking a build-tree lease.
`workflow run` accepts repeated `--only NODE` selections and `--jobs N`; it
writes `run.json` and per-node stdout/stderr logs. The `report` command only
reads that journal and formats node status, durations, process results,
verification, artifacts, experiment results and uncertainty. It does not
contact CMake or modify the run directory.

The existing target/bridge interface remains available:

```sh
DagFlow.Tools context inspect out/build/core
DagFlow.Tools target build --build-dir out/build/core --target dagflow_example
DagFlow.Tools target invoke --build-dir out/build/core --target run_dagflow_runtime_campaign
DagFlow.Tools target run --build-dir out/build/core --target dagflow_example
DagFlow.Tools workflow sequence --build-dir out/build/core --first dagflow_example --second dagflow_example_task_scope
```

`target build` accepts native executable/library targets. `target invoke` is the
explicit route for arbitrary CMake custom targets. `target run --prepared` is
for a CMake bridge target that already declares the native target as a
dependency; it does not enter Ninja recursively. DagFlow's generated bridge
invokes `target run --prepared` internally. Do not call it as a substitute for
a regular `target run` unless the caller has already completed that native
dependency edge.

NativeAOT publishing uses the project RID:

```sh
dotnet publish tools/DagFlow.Tools/DagFlow.Tools.csproj -c Release -r linux-x64 -o out/tools/workflow-aot
```

## Maintained examples

- [`workflows/tests-managed.json`](../workflows/tests-managed.json) explicitly
  configures `core`, invokes the actual `dagflow_tests` aggregate to build its
  test executables, then runs the `unit` CTest label.
- [`workflows/tests-adopt.json`](../workflows/tests-adopt.json) runs the same
  target and CTest selection in an existing IDE tree, with explicit
  `allowUnverified` adoption and no configure node.
- [`workflows/benchmarks-smoke.json`](../workflows/benchmarks-smoke.json) builds
  the native `dagflow_public_api_bench` and `dagflow_runtime_suite` executables,
  then runs bounded chain, parallel-for, deep-DAG and steal-contention cases.
  The binaries print their own measurement JSON into the journal logs.
- [`workflows/benchmarks-campaign.json`](../workflows/benchmarks-campaign.json)
  intentionally invokes the existing `run_dagflow_api_campaign` and
  `run_dagflow_runtime_campaign` harness targets from the `runtime-campaign`
  preset. Measurements remain in the CMake harness output tree; DagFlow.Tools
  reports command outcomes, not benchmark measurements.
- [`workflows/validation-asan.json`](../workflows/validation-asan.json) and
  [`workflows/validation-tsan.json`](../workflows/validation-tsan.json) invoke
  `dagflow_tests` then select unit tests under the existing sanitizer presets.
- [`workflows/validation-fuzz-smoke.json`](../workflows/validation-fuzz-smoke.json)
  builds the existing `dagflow_fuzz_queues` executable and selects its bounded
  CTest smoke test. It does not invoke the longer `fuzz_dagflow_fuzz_queues`
  campaign target.

For example, inspect or run a maintained workflow with:

```sh
DagFlow.Tools workflow plan workflows/tests-adopt.json
DagFlow.Tools workflow run workflows/tests-adopt.json --only unit-tests
DagFlow.Tools workflow report out/workflow-results/<run-directory>
```

The benchmark and fuzz entries are explicit execution requests and may take
longer than unit tests. The campaign workflow retains the native harness's
cases, arguments, timeouts, affinity and measurement policy. On the current
Linux x86-64 checkout, both API and runtime campaign targets complete and keep
their measurements in the harness output tree. `HarnessCMake.cmake` accepts
scientific-notation JSON metrics and converts them to the existing fixed-point
micro-unit representation; `dagflow_campaign_tests` covers positive and
negative exponents. The current API smoke campaign took about nine minutes on
the local host, so it remains a separate explicit workflow; the bounded
executable smoke workflow is the quick check. The workflow ceilings exceed the
native harness's 120-second per-process timeout across all configured rounds.

## Limitations and supported contract

- There is no portable native build freshness predicate.
- Environment hermeticity is not attested.
- Preset identity is not inferred from CMakeCache.
- Leases coordinate DagFlow.Tools processes only; they cannot lock out an IDE.
- CMake regeneration may execute native build-system side effects.
- DagFlow.Tools does not reconstruct CMake dependencies or cache workflow
  results for reuse.
- Schema v2 is the supported contract; v1 is retained for compatibility.
- NativeAOT and CMake/Ninja integration jobs are configured in GitHub Actions
  for Linux, Windows and macOS. Linux x86-64 NativeAOT publish and all local
  integration drivers passed; Windows and macOS remain unverified until those
  hosted jobs complete.
