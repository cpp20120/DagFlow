# Building and running benchmarks with CMake

> **Historical workflow note (October 2026).** This report preserves its original
> benchmark commands and measurements. The former `scripts/benchmark_*.py`
> orchestration is retired; those commands are not part of the current build.
> For reproducible runs use [CMake benchmark campaigns](campaigns.md).


CMake owns benchmark source lists, dependencies, allocator selection and build
profiles. Python harnesses select those profiles, run workload matrices and save
reports. No benchmark or PGO training runs during an ordinary build.

DagFlow project switches use the `DAGFLOW_` prefix, including
`DAGFLOW_BUILD_SHARED`, `DAGFLOW_BUILD_STATIC`, `DAGFLOW_BUILD_TESTS` and
`DAGFLOW_INSTALL`. Generic profile, policy and harness switches use the
framework's `DAGFLOW_` prefix. Reconfigure existing build directories with
their preset after updating; old cached option names are no longer read.
Historical experiment runners translate option names only when building a
frozen source tree that still declares the old CMake API.

The rules are split by responsibility: the vendored `cmake/` tree
declares project capabilities, target policies, profiles, workloads and the
process harness. The small DagFlow modules only describe runtime sources,
allocator linkage, benchmarks and project-specific checks. Historical
experiment scripts can retain compiler flags for frozen source trees predating
this layout; changing their archived build recipe would change the experiment.

## Build everything

```sh
cmake --preset bench-all
cmake --build --preset bench-all --target dagflow_benchmarks --parallel 4
```

This uses Clang, Release, the system allocator and static DagFlow. It includes
oneTBB, so its development package must be installed. `bench-release` builds the
DagFlow benchmarks without the oneTBB dependency. Presets use separate directories
under `out/build/<preset>` and do not need artifact suffixes.

Build and run individual executables directly:

```sh
cmake --build --preset bench-all --target dagflow_runtime_bench
out/build/bench-all/dagflow-runtime-bench 4 4096 9

cmake --build --preset bench-all --target dagflow_runtime_suite
out/build/bench-all/dagflow-runtime-suite --workers 4 --tasks 4096 \
  --repeats 9 --warmup 2 --work-ns 1000

out/build/bench-all/dagflow-public-api-bench --benchmark noop --workers 4 --runs 5
out/build/bench-all/dagflow-tbb-bench --benchmark noop --workers 4 --runs 5
out/build/bench-all/dagflow-stress-bench --scenario local-overflow \
  --workers 4 --tasks 4096 --repeats 9 --json
```

| Target | Executable | Configure option |
| --- | --- | --- |
| `dagflow_runtime_bench` | `dagflow-runtime-bench` | `DAGFLOW_BUILD_RUNTIME_BENCH` |
| `dagflow_runtime_suite` | `dagflow-runtime-suite` | `DAGFLOW_BUILD_RUNTIME_SUITE` |
| `dagflow_stress_bench` | `dagflow-stress-bench` | `DAGFLOW_BUILD_STRESS_BENCH` |
| `dagflow_public_api_bench` | `dagflow-public-api-bench` | `DAGFLOW_BUILD_PUBLIC_API_BENCH` |
| `dagflow_tbb_bench` | `dagflow-tbb-bench` | `DAGFLOW_BUILD_BENCH` |
| `dagflow_function_bench` | `dagflow-function-bench` | `DAGFLOW_BUILD_FUNCTION_BENCH` |
| `dagflow_github_suite` | `dagflow-github-suite` | `DAGFLOW_BUILD_GITHUB_BENCH` |

Each executable has its own `main()`. The TBB target links TBB and Threads only;
it does not link DagFlow. Runtime suite defaults to enabled when the legacy
`DAGFLOW_BUILD_RUNTIME_BENCH` switch is enabled in a fresh build directory, and can be
selected independently. Existing distribution presets and `DagFlow_example`
remain available; the stress harness now also has an explicit benchmark target.
Allocator packages are only required when building the DagFlow runtime or its
tests; a standalone TBB or GitHub-snapshot build does not require mimalloc.

Each benchmark also has an explicit CMake run target. Configure its arguments,
then build that target to build and execute the selected benchmark:

```sh
cmake --preset bench-release \
  -DDAGFLOW_RUNTIME_SUITE_ARGS="--scenario steal_heavy --workers 8 --tasks 4096 --iterations 0 --repeats 9"
cmake --build --preset bench-release --target dagflow_run_runtime_suite
```

The other run targets are `dagflow_run_runtime_bench`, `dagflow_run_stress_bench`,
`dagflow_run_public_api_bench`, `dagflow_run_tbb_bench`,
`dagflow_run_function_bench` and `dagflow_run_github_suite`. Their argument
variables use the same stem in uppercase, for example `DAGFLOW_TBB_BENCH_ARGS`.
Run timed targets one at a time to avoid measuring interference between them.

## Profiles

| Preset | Configuration |
| --- | --- |
| `bench-release` | Release (`-O3 -DNDEBUG` with Clang), no LTO |
| `bench-lto` | Release + ThinLTO |
| `bench-full-lto` | Release + full LTO |
| `bench-native-thinlto`, `bench-native-full-lto` | Corresponding LTO + native CPU optimization |
| `bench-o3`, `bench-o3-lto` | Release + debug symbols; second uses full LTO, matching the stress harness |
| `bench-diagnostics` | O3 with runtime counters; unsuitable as a timing baseline |
| `bench-all` | Release plus the oneTBB target |
| `bench-github` | Release plus an explicitly selected external GitHub snapshot |
| `bench-check` | All benchmarks and correctness tests |
| `check-asan`, `check-tsan` | Debug correctness builds with ASan/UBSan or TSan |

Every configure preset has a matching build preset. Example:

```sh
cmake --preset bench-lto
cmake --build --preset bench-lto --target dagflow_benchmarks --parallel 4
```

The same profile definitions work without presets:

```sh
cmake -S . -B out/build/my-suite -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ -DDAGFLOW_PROFILE=lto \
  -DDAGFLOW_USE_LLD=ON -DDAGFLOW_ALLOCATOR=system \
  -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_BUILD_STATIC=ON -DDAGFLOW_INSTALL=OFF \
  -DDAGFLOW_BUILD_EXAMPLES=OFF -DDAGFLOW_BUILD_RUNTIME_SUITE=ON
cmake --build out/build/my-suite --target dagflow_runtime_suite
```

`DAGFLOW_PROFILE` selects the build type, LTO and PGO modes. Use `custom` (the
default) to control those switches individually. Allocators remain independent:
`system`, `mimalloc`, `tbbmalloc`; the latter two require their development
packages. ThinLTO presets require Clang. Legacy shared/static/native distribution
presets remain in `CMakePresets.json`.

## PGO without Python

Training is an explicit target, separate from timing results:

```sh
cmake --preset bench-pgo-generate
cmake --build --preset bench-pgo-generate --target dagflow_pgo_merge --parallel 4
cmake --preset bench-pgo-use
cmake --build --preset bench-pgo-use --target dagflow_benchmarks --parallel 4
```

`dagflow_pgo_merge` builds the enabled current-runtime benchmarks, runs bounded
training (including both suite modes), then merges `.profraw` files using
`llvm-profdata`. Training covers each executable's distinct `main()`; the public
API/TBB trainers use the chain workload. This is a reproducible starting profile,
not a substitute for training on the intended production workload.
`dagflow_pgo_train` runs training without merging. For ThinLTO + PGO use
`bench-lto-pgo-generate` and `bench-lto-pgo-use` instead. Each pair has a separate
profile directory under `out/pgo/`. Configure of the use phase fails clearly if
the required merged profile is missing. Start with a new profile directory when
changing training sources; raw files in an existing directory are all merged.
The merged profile is an explicit object dependency, so updating it at the same
path also rebuilds the PGO-use objects. Dependencies are assigned after all
targets have their final source lists, including later `target_sources()` calls.
`dagflow_pgo_merge_only` merges existing samples after a custom workload without
running the bundled trainer. Enabled examples are also trained by the default
target. Training and merging run through the explicit CMake targets `dagflow_pgo_train` and `dagflow_pgo_merge`.

## Correctness checks

```sh
cmake --preset bench-check
cmake --build --preset bench-check --parallel 4
ctest --preset bench-check
ctest --preset bench-check -L benchmark
```

The benchmark label selects short executable smoke checks, without speed
thresholds. The full suite also checks workload checksums and harness output.
`check-asan` and `check-tsan` have corresponding CTest presets. In a ptrace sandbox
where LeakSanitizer cannot run, set `ASAN_OPTIONS=detect_leaks=0` explicitly;
leak detection is not disabled by the preset itself.

## Local GitHub snapshot

```sh
cmake --preset bench-github \
  -DDAGFLOW_GITHUB_SOURCE_DIR="$PWD/out/benchmarks/github-comparison/source/github"
cmake --build --preset bench-github --target dagflow_github_suite dagflow_runtime_suite
out/build/bench-github/dagflow-github-suite --scenario external_handles \
  --workers 4 --tasks 4096 --iterations 0 --repeats 9 --warmup 2
```

Legacy sources are not kept in the working tree. Set `DAGFLOW_GITHUB_SOURCE_DIR`
to the snapshot saved with a measurement run, or another external directory. The
adapter maps API spelling and does not modify the snapshot runtime. Recursive
TaskScope is unavailable there. Graph reuse is checked as-is; the supplied
snapshot rejects its second run. `--fresh-graph` constructs each graph before
the timed interval when comparing other graph scenarios.

For paired matrices and saved provenance, the optional runner uses these same
CMake targets and profiles:

```sh
python3 scripts/benchmark_github.py \
  --legacy out/benchmarks/github-comparison/source/github \
  --output out/benchmarks/github-comparison-new
```

## Validation of this layout

- All 32 non-hidden presets configured successfully in isolated directories;
  PGO-use configurations were supplied with an existing merged profile.
- `bench-check`: all six benchmark executables built, 29/29 CTest checks passed.
- Shared ThinLTO and native full-LTO runtime builds and short runs passed.
- Both PGO pairs completed training/merge/use; all enabled benchmark targets
  built without profile-mismatch warnings after training each executable.
- ASan/UBSan and TSan presets each passed the runtime-suite smoke, ownership and
  TaskScope checks (3/3). ASan used `detect_leaks=0` for the sandbox restriction.
- The public API and TBB Python wrappers built through CMake. Tiny runtime-suite
  PGO/LTO-PGO and stress-harness matrices also completed through their new CMake
  paths. These are integration checks, not performance comparisons.
- A merged-profile timestamp change was checked to schedule recompilation of
  both the runtime objects and suite object in the PGO-use build.

Full measurement results for the separate snapshot comparison are in
[GitHub comparison](github-comparison.md).

A follow-up CMake cleanup compared the same 32 presets against a source snapshot:
compiler flags and translation-unit lists were preserved. The new
`dagflow_cmake_configuration_tests` checks unused allocator dependencies,
configuration validation without runtime targets, and late-added PGO sources.
It runs under Clang with Ninja and has the CTest label `build`.
