# CMake benchmark campaigns

DagFlow vendors `cmake_boilerplate/lib/cmake` unchanged under `cmake/boilerplate/`.
The root CMake project describes the runtime, allocator, tests and workloads.
Build profiles, target policies, PGO, documentation, packaging and the process
harness use the toolkit's `boilerplate_*` API and `BOILERPLATE_*` options.
DagFlow component selection and allocator options remain `DAGFLOW_*`.
Use a fresh build directory when migrating from the old renamed framework.

## Build, test, document

```sh
cmake --preset debug
cmake --build --preset debug --parallel 4
ctest --preset debug --output-on-failure

cmake --preset release -DDAGFLOW_BUILD_DOCS=ON
cmake --build --preset release --target docs
```

CMake, a C++23 compiler and Ninja suffice for the system-allocator presets.
Documentation additionally requires Doxygen. Python is not used by the build,
CTest, campaigns, result comparison or documentation generation.

## Run campaigns

```sh
cmake --preset runtime-campaign
cmake --build --preset runtime-campaign

# All enabled families, executed sequentially even with a parallel build:
cmake --build out/build/runtime-campaign --target dagflow_campaigns --parallel 4
```

| Target | Required component |
| --- | --- |
| `run_dagflow_runtime_campaign` | `DAGFLOW_BUILD_RUNTIME_SUITE=ON` |
| `run_dagflow_stress_campaign` | `DAGFLOW_BUILD_STRESS_BENCH=ON` |
| `run_dagflow_api_campaign` | `DAGFLOW_BUILD_PUBLIC_API_BENCH=ON` |
| `run_dagflow_tbb_campaign` | `DAGFLOW_BUILD_BENCH=ON` and installed oneTBB |
| `run_dagflow_github_campaign` | `DAGFLOW_BUILD_GITHUB_BENCH=ON` and `DAGFLOW_GITHUB_SOURCE_DIR` |

Campaign targets are explicit; ordinary builds never execute benchmarks.
`DAGFLOW_BUILD_HARNESS=ON` registers the campaigns for enabled components.

Generated cases live in `<build>/campaigns/`. The default `smoke` group runs
one worker/payload setting per scenario. The `full` group covers the worker,
fixed-payload and throughput/latency matrix, omitting the unsupported
single-worker stealing case and the legacy snapshot's recursive scope case.

```sh
cmake --preset runtime-campaign \
  -DDAGFLOW_CAMPAIGN_GROUP=full \
  '-DDAGFLOW_CAMPAIGN_WORKERS=1;2;4;8' \
  '-DDAGFLOW_CAMPAIGN_ITERATIONS=0;64' \
  -DDAGFLOW_CAMPAIGN_TASKS=4096 \
  -DDAGFLOW_CAMPAIGN_REPEATS=9 \
  -DDAGFLOW_CAMPAIGN_WARMUP=2 \
  -DDAGFLOW_CAMPAIGN_ROUNDS=5
cmake --build --preset runtime-campaign
```

Payload iterations are fixed across processes and builds. Automatic `--work-ns`
calibration on every invocation would change the workload and checksum, making
repeated measurements incomparable. For a calibrated workload, run the suite's
`--calibrate` once and use the resulting iteration count in the cases file.

Each campaign writes `manifest.json`, `runs.jsonl`, `summary.json` and raw logs
under `<build>/harness-results/<config>/dagflow_<family>_campaign/`.
The manifest records binary SHA256, compiler, policies, allocator and diagnostics.
Metrics in `summary.json` use integer micro-units (`scale: 1000000`); divide by
that scale to obtain the executable's original metric units. Checksums are checked
across measured processes within each case. Runtime correctness tests also check
checksums across worker counts and execution modes.

Direct `run_*` targets reuse their output directory. Use a new
`BOILERPLATE_HARNESS_RESULTS_DIR` for a preserved measurement, or the matrix runner
below, which refuses to overwrite an existing output directory.

## Custom cases, batches, shards and diagnostics

Pass `DAGFLOW_RUNTIME_CASES`, `DAGFLOW_STRESS_CASES`, `DAGFLOW_API_CASES`,
`DAGFLOW_TBB_CASES` or `DAGFLOW_GITHUB_CASES` to select your own JSON cases file.
The toolkit supports shared defaults, named groups, environment, CPU placement
and metadata. Set `DAGFLOW_CAMPAIGN_GROUP=` for a plain JSON array without groups.
For example, this runtime case measures a partially filled final submission batch:

```json
[
  {
    "name": "external-batch-16",
    "args": ["--scenario", "external_detached", "--workers", "4",
             "--shards", "2", "--submit-batch", "16", "--central-batch", "4",
             "--tasks", "4097", "--iterations", "64", "--repeats", "9", "--warmup", "2"]
  }
]
```

Use `DAGFLOW_RUNTIME_DIAGNOSTICS=ON` in a separate build for allocation counters.
All fields emitted by the executable, including per-run diagnostics and latency
samples, remain in `runs.jsonl`; timing comparisons should use uninstrumented builds.
Graph fresh/reuse semantics belong to the cases (`--fresh-graph`), not the runner.

## Compare profiles, allocators or revisions

```sh
cmake '-DPRESETS=bench-release;bench-lto' \
  '-DALLOCATORS=system;mimalloc;tbbmalloc' \
  -DOUTPUT_DIR=out/campaigns/allocator-comparison \
  -DCAMPAIGN_TARGET=run_dagflow_runtime_campaign \
  '-DCONFIGURE_ARGS=-DDAGFLOW_CAMPAIGN_GROUP=full;-DDAGFLOW_CAMPAIGN_ROUNDS=5' \
  -P cmake/BenchmarkDagFlow.cmake
```

Install the allocator packages before selecting mimalloc or tbbmalloc. Each
preset/allocator pair gets an independent build and result directory. Use
`SOURCE_DIR` to run another checkout; use the same cases and payload iterations
for baseline and candidate. The same runner handles the current/legacy suite
when the GitHub snapshot is explicitly configured.

```sh
cmake -DBASELINE=/path/before/summary.json -DCANDIDATE=/path/after/summary.json \
  -DMETRIC=run_p50_us -DOUTPUT=comparison.csv -P cmake/CompareDagFlow.cmake
```

The comparator rejects failed runs, differing case sets, scales or invariants.
`MAX_REGRESSION_PERCENT` optionally enforces a timing threshold; pass
`HIGHER_IS_BETTER=ON` when comparing throughput. Supply identical API cases to
both DagFlow and oneTBB (oneTBB has no `independent-batch` case).

PGO retains the existing presets and uses the toolkit targets:

```sh
cmake --preset bench-pgo-generate
cmake --build --preset bench-pgo-generate --target boilerplate_pgo_merge --parallel 4
cmake --preset bench-pgo-use
cmake --build --preset bench-pgo-use --parallel 4
```

## Optional profiling

`DAGFLOW_CAMPAIGN_AFFINITY=physical` requests physical-core placement.
`DAGFLOW_CAMPAIGN_PERF_EVENTS=cycles;instructions;cache-misses` requests Linux
`perf stat`; the toolkit retains raw counter files. These counters cover the
whole process and should be collected separately from primary timing runs.

The specialized `profile_main.py`, `profile_main_stacks.py`, `perf_flamegraph.py`
and `render_main_profile.py` remain optional Python tools in `tools/profiling/`. They use the current
CMake options and are not imported or discovered by ordinary workflows.

## Replacements for the old Python commands

| Previous workflow | Current entry point |
| --- | --- |
| `benchmark_all.py`, `benchmark_allocators.py` | `cmake -P cmake/BenchmarkDagFlow.cmake` |
| `benchmark_suite.py`, `benchmark_main.py` | Runtime/stress campaign targets |
| `benchmark_public_api.py`, `benchmar_tbb.py` | API/TBB campaign targets |
| `benchmark_github.py` | Runtime/GitHub campaigns with identical custom cases |
| `benchmark_batches.py`, graph/drain/wake comparisons | Custom cases in baseline/candidate builds; `CompareDagFlow.cmake` |
| `measure_graph_allocations.py` | Diagnostics build; raw counters in `runs.jsonl` |
| `docs/generate_docs.py` | `DAGFLOW_BUILD_DOCS=ON`, target `docs` |
| Python CLI/schema tests | CTest scripts and native Linux perf-protocol test |

The old bespoke CSV/HTML report schemas are not reproduced. Historical reports
retain their original measurements; new campaigns use the toolkit result format.
