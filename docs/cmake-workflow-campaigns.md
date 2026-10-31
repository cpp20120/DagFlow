# DagFlow campaigns with CMakeWorkflow

The [CMakeWorkflow](https://github.com/cpp20120/CMakeWorkflow) .NET 10 CLI
runs DagFlow's mutation, Relacy weak-memory, fuzz-corpus and profiling campaigns
outside the DagFlow source tree. Build the CLI in its own checkout:

```sh
dotnet build CMakeWorkflow.slnx -c Release
dotnet publish CMakeWorkflow.csproj -c Release -r linux-x64 -o out/native
```

Add the published directory to `PATH`. The commands below run from the
DagFlow root. Use the RID appropriate for the host when publishing. Normal
DagFlow configure, build and CTest commands do not require .NET or this CLI.
CMake still owns native dependencies, compilation and test registration.

## Mutation and weak memory

```sh
CMakeWorkflow dagflow mutation run --suite graph --source-dir .
CMakeWorkflow dagflow mutation run --suite queue --source-dir .
CMakeWorkflow dagflow weak-memory run --protocol parking \
  --relacy out/weak-memory/relacy --source-dir .
```

Mutation compiles disposable C++ variants and runs the property models.
Weak-memory campaigns compile versioned Relacy adapters and verify source
hashes before running. Changed DagFlow sources require an updated adapter
catalog. Mutation and weak-memory compilation currently uses GNU/Clang flags
on Unix hosts.

## Fuzz corpus

```sh
CMakeWorkflow dagflow fuzz seed --layer ranges --source-dir .
CMakeWorkflow dagflow fuzz run --build-dir out/build/fuzz --layer ranges \
  --duration 60 --results-dir out/fuzz-runs
CMakeWorkflow dagflow fuzz triage --run-dir out/fuzz-runs/<run-directory>
CMakeWorkflow dagflow fuzz merge --run-dir out/fuzz-runs/<run-directory>
```

Seed generation writes named seeds and refuses to overwrite different bytes.
`fuzz run` builds the selected CMake File API target, copies checked-in seeds
and accumulated inputs to a separate writable corpus, replays inputs, then
starts the selected libFuzzer, AFL++ or honggfuzz backend. It retains build
logs, input hashes, corpus changes and crash artifacts. `fuzz triage` replays
findings against the recorded executable hash. `fuzz merge` imports a
successful run's inputs into the build-tree corpus by content hash. Checked-in
seeds are not changed by default.

For libFuzzer, `fuzz merge --coverage --run-dir RUN_DIRECTORY` uses native
coverage-guided corpus merging and retains its log. Crash replay writes a
separate log for each finding. A LeakSanitizer failure caused by unavailable
ptrace is reported as an environment error.

## Profiling

```sh
CMakeWorkflow dagflow profile run --build-dir out/build/bench-o3 \
  --target dagflow_stress_bench --backend auto --mode cpu \
  --results-dir out/profiles -- --workers 1 --scenario idle-burst

CMakeWorkflow dagflow profile run --campaign main-o3-lto \
  --o3-build-dir out/build/bench-o3 --lto-build-dir out/build/bench-o3-lto \
  --target dagflow_stress_bench --repeats 3 --results-dir out/profiles

CMakeWorkflow dagflow profile analyze out/profiles/<run-directory>
```

The command resolves the executable through the CMake File API, asks CMake to
build it, then executes uninstrumented timing and platform capture as separate
operations in the same workflow graph. `--preset` explicitly adds a configure
operation; otherwise an existing build tree is adopted. An adopted tree must
already expose a File API codemodel. Create a `codemodel-v2` query under
`.cmake/api/v1/query/` before configuring if the selected preset does not
create one.

Repeated `--variant name=BUILD_DIR` arguments define a comparison matrix;
`main-o3-lto` is a shorthand. Analysis writes `comparison.json` with timing
samples, medians and ratios to the first variant, and
`counter-comparison.json` for matching Linux perf events. Schema 2 workflow
manifests can also declare profile operations with a successful build in
`from`.

The `--backend` choices are `auto`, `perf`, `etw` and `instruments`.
`--mode` selects `cpu`, `scheduler`, Linux `counters`, or macOS
`allocations` as supported by the backend. Linux counters use `perf stat` CSV
and remain separate from CPU sampling. Profiling retains raw traces, capture
manifests, results, `runs.jsonl` and normalized metadata. Perf PMU counters,
ETW CPU samples and Instruments samples have different event sources and units.

WPR records system-wide data, so the ETL summary filters by target PID and
execution window. It records CPU sample, context-switch and ready-event counts,
observed ready and running intervals, per-thread counts and target sample
JSONL. Raw stack addresses are preserved when present; symbolized call trees
still require WPA or another symbol-aware analyzer. The backend uses a unique
WPR session name and cleans up after failure or cancellation.

CMakeWorkflow executes the resolved benchmark itself for profiling. It does not
launch a CMake `run_*` target during capture, which would create a second
process controller. DagFlow's standalone CMake benchmark harness remains
usable without the CLI.
