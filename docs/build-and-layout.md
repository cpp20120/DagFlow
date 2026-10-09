# Build and repository layout

DagFlow is a C++23 library. CMake 3.26+ configures the project. Build, test,
benchmark and packaging infrastructure lives in `cmake/dagflow/`; runtime and
workload definitions live in `cmake/DagFlow*.cmake`. All public build settings
use `DAGFLOW_*` and CMake helpers use `dagflow_*`.

```text
include/dagflow/         installed public headers and implementation-detail headers
src/                     library translation units only
examples/                minimal executable API examples
bench/                   stress harness, runtime cases and benchmarks
tests/                   runtime unit, schema, regression and packaging tests
cmake/DagFlow*.cmake     DagFlow project glue and benchmark campaigns
cmake/dagflow/       DagFlow build, test and benchmark infrastructure
tools/profiling/         optional Python Linux-perf analysis only
docs/                    design notes and historical measurements
```

## Host setup and build entry points

Linux/macOS: `./setup.sh --run`. Windows PowerShell:
`powershell -NoProfile -ExecutionPolicy Bypass -File .\setup.ps1 -Run`.
The default preset is `core`; setup prepares the host, then delegates to
`build.sh` / `build.ps1` for configure, build, CTest and the requested example.
Subsequent builds can call that build script directly. Both scripts activate
the saved environment in `out/host-tools/env.sh` or `env.ps1` without editing
shell startup files.

Existing compatible tools are reused. Missing tools are installed through the
host package manager; setup also handles CMake versions below 3.26. macOS may
need the Command Line Tools installer; Windows may need Visual Studio Build
Tools installation and an OS-requested restart. Initial downloads need network
access, and system installations can require administrator authorization.

| Task | Bash | PowerShell arguments to `setup.ps1` |
| --- | --- | --- |
| Prepare tools only | `./setup.sh --setup-only` | `-SetupOnly` |
| Preview actions | `./setup.sh --dry-run` | `-DryRun` |
| Core tests and example | `./setup.sh --run` | `-Run` |
| Full benchmark smoke tests | `./setup.sh --preset bench-check` | `-Preset bench-check` |
| Developer tools | `./setup.sh --profile dev --setup-only` | `-Profile dev -SetupOnly` |
| TGZ archive and install tree | `./setup.sh --package-format TGZ --install-artifacts` | `-PackageFormat ZIP -InstallArtifacts` |

Archives go to `out/packages/<preset>/`; installations go to
`out/artifacts/<preset>/`. Packaging enables DagFlow's install/export options.
`--run` runs the core example; benchmark presets use an explicit target, e.g.
`./build.sh --preset runtime-check --run-target dagflow_run_runtime_suite`.

The default `minimal` host profile supplies build prerequisites. `dev` adds
formatting, analysis and documentation tools; `ci` adds instrumentation tools;
`package` adds native packaging tools where available. Compiler-specific presets
still need the appropriate compiler: for example,
`./setup.sh --profile ci --preset fuzz` supplies Clang tooling. Cross compilation
requires the target SDK, and PGO-use requires a previously trained profile.

The entry points, host discovery, default preset and tool manifest are maintained
inside DagFlow. See [integration boundaries](cmake-integration.md).

## Fast, dependency-free build

The `core` preset builds the library, examples and tests with system allocation.
It does not search for vcpkg, oneTBB or mimalloc, or download anything. CMake
chooses the compiler and native generator; no Ninja/Clang requirement is imposed.

```sh
cmake --preset core
cmake --build --preset core
ctest --preset core
```

Use `runtime-check` for the standalone runtime, scenario suite, public API,
stress and callable benchmarks plus CTest. Use `bench-check` to add oneTBB.
Both have matching configure/build/test presets. Named profiles work with
single-config and multi-config generators; build/test presets select the
matching configuration on Visual Studio as well.

`DAGFLOW_BUILD_RUNTIME_BENCH` also selects the scenario suite by default.
For the preconfigured compiler/profile matrices use `cmake --list-presets`,
`cmake --preset <name>`, `cmake --build --preset <name>` and
`ctest --preset <name>`.

To build all optional C++ benchmark programs, including oneTBB comparison,
configure `bench-all`; oneTBB is provisioned automatically through vcpkg. `dagflow_benchmarks` builds
without executing them. To run a particular benchmark explicitly, build its
`dagflow_run_*` target. See [CMake campaigns](benchmarks/campaigns.md) for
cases, JSON results, comparison and PGO. Normal builds do not execute
benchmark campaigns or invoke Python.

## Dependency and toolchain controls

`cmake/DagFlowDependencies.cmake` maps enabled components to `vcpkg.json`
features **before** `project()`: `tbb-bench`, `mimalloc`, `tbbmalloc`. An unused
allocator does not pull a dependency into a benchmark-only build. The manifest's
`builtin-baseline` pins package versions and the managed vcpkg checkout.

- `DAGFLOW_DEPENDENCY_PROVIDER=auto` is DagFlow's default: system for core,
  vcpkg when an external dependency is requested. Direct `-D` options and presets
  use the same selection. Explicit `VCPKG_MANIFEST_FEATURES` are preserved.
- `DAGFLOW_DEPENDENCY_PROVIDER=system` uses packages already supplied by the
  caller (including their toolchain/prefix); it never downloads vcpkg.
- Explicit `CMAKE_TOOLCHAIN_FILE`, `DAGFLOW_VCPKG_ROOT` and `VCPKG_ROOT` take
  priority. With no override, `DAGFLOW_VCPKG_BOOTSTRAP=ON` provisions
  `<build>/_deps/vcpkg-<commit>` and the official toolchain installs into
  `<build>/vcpkg_installed`. A bad explicit root is an error.
  The build scripts set the shared checkout cache to `out/host-tools/vcpkg`.
- `DAGFLOW_VCPKG_BOOTSTRAP=OFF` restores the framework's discovery-only
  behavior. `DAGFLOW_VCPKG_CACHE` shares managed checkouts between builds;
  `DAGFLOW_VCPKG_REPOSITORY` accepts a Git mirror. Package binary/download
  caches use vcpkg's standard settings. An offline first build needs those
  packages/sources and the vcpkg executable already cached.

Use a fresh binary directory when changing toolchains or dependency providers.
For cross compilation supply a vcpkg triplet and `VCPKG_CHAINLOAD_TOOLCHAIN_FILE`;
the project does not guess the target platform. `add_subdirectory` never replaces
the parent's toolchain or manifest; an embedding application supplies its optional
allocator dependencies. The default embedded runtime requires only Threads.

LLD is not imposed by standard presets. Requested LTO, native ISA, sanitizers
and PGO undergo a compiler **and linker** probe. LTO tries the platform linker
first and uses LLD only if the fallback probe succeeds. ARM native builds use
`-mcpu=native`; cross builds reject host-native optimization. Unsupported explicit
instrumentation fails during configure with a diagnostic rather than silently
turning off the requested checks. ThinLTO/libFuzzer require Clang; TSan needs a
supported runtime; PGO-use needs a trained profile. The external snapshot preset
`bench-github` also needs `DAGFLOW_GITHUB_SOURCE_DIR` because the comparison input
cannot be inferred. Optional Doxygen/perf tools remain host tools, not core deps.

The CI matrix independently checks core and runtime with package discovery
forbidden, and oneTBB/mimalloc/tbbmalloc with managed vcpkg on Linux/macOS/Windows.
It verifies the package origin, runs CTest and tests the installed core consumer.

## Public headers and library consumers

Library and client code includes public headers using their exported path:

```cpp
#include <dagflow/dagflow.hpp>
#include <dagflow/detail/scheduler.hpp> // for internal tests and runtime code only
```

The `detail/` path is *not* a stability or compatibility promise. Local-only
headers (`tests/support.hpp`, `bench/runtime_suite_backend.hpp`) use quoted
includes; the legacy snapshot benchmark keeps its own bare header names.

The in-tree entry points are `DagFlow::DagFlow`,
`DagFlow::DagFlow_shared` and `DagFlow::DagFlow_static` when available;
`DagFlow::shared` and `DagFlow::static` are also emitted by the toolkit.
Installation exports `DagFlow::DagFlow` and explicit `DagFlow::DagFlow_shared` /
`DagFlow::DagFlow_static` aliases when the corresponding variants exist.

```cmake
add_subdirectory(external/DagFlow)
target_link_libraries(my_program PRIVATE DagFlow::DagFlow)
```

For an installed distribution, use `find_package(DagFlow CONFIG REQUIRED)`
and link the same name. Library code requires the C++23 language level.

## Profiles, allocation and diagnostics

`DAGFLOW_ALLOCATOR` is `system` (default), `mimalloc` or `tbbmalloc`.
The external packages are required only for the chosen allocator or the
oneTBB comparison target. `DAGFLOW_PROFILE`,
`DAGFLOW_SANITIZER`, and `DAGFLOW_PGO_MODE` belong to the framework;
`DAGFLOW_*` options toggle DagFlow components.

PGO uses explicit `dagflow_pgo_train` / `dagflow_pgo_merge` steps;
profile generation and use are separate builds. Documentation uses
`DAGFLOW_BUILD_DOCS=ON` and target `docs`, subject to Doxygen availability.
The optional `tools/profiling/*.py` utilities perform specialized Linux
`perf`/flamegraph analysis and are not dependencies of CMake/CTest.

## Fuzzing

`cmake --preset fuzz`, `cmake --build --preset fuzz`, and `ctest --preset fuzz`
exercise eighteen active layers through the dagflow fuzz API. The private
fuzz runtime receives coverage and sanitizer instrumentation without changing
normal library targets. See [fuzz layers and campaigns](../fuzz/README.md).

The thirteen [adversarial runtime regressions](../tests/adversarial-tests.md)
are ordinary CTest tests under the `adversarial` label. Build them together with
target `dagflow_adversarial_tests`; preset `adversarial-tiny` runs the same group
with two-slot queues and a central batch of one.


## Library infrastructure and CI

The infrastructure under `cmake/dagflow/` is maintained as part of DagFlow.
It supports the runtime, tests, benchmark workloads, harness, fuzz layers and
library exports. GPU, UI and application-deployment modules have been removed.
The independent upstream project's lifecycle is described in
[CMake integration](cmake-integration.md).

### Presets and targets

| Purpose | Command | Details |
| --- | --- | --- |
| Cross-platform core | `cmake --workflow --preset core` | Configure, compile and test with system allocator |
| CI-oriented debug | `cmake --workflow --preset ci-debug` | No compiler cache; project `ci` capability |
| GCC coverage | `cmake --workflow --preset coverage` | Unit/adversarial tests compiled with gcov coverage flags |
| Coverage HTML | `cmake --build --preset coverage --target coverage-report` | Requires host `lcov` and `genhtml`; report in `out/build/coverage/coverage/html/` |
| Release bundles | `cmake --build --preset distribution --target package` | Shared + static, install/export, CPack TGZ/DEB (Linux), ZIP (Windows), TGZ/DMG (macOS if available) |
| Clang Linux | `cmake --workflow --preset native-linux-clang` | Native Clang compiler and tests |
| AArch64 cross | `cmake --preset linux-arm64 && cmake --build --preset linux-arm64` | Debian cross GCC, static lib; cannot execute without target runtime/emulation |
| Optional targets | `cmake --build --preset core --target format-check-all` | Available when `clang-format` / `cmake-format` are installed; does not rewrite files |
| Static analysis | `cmake --build --preset core --target analyze-clang-tidy` | Requires `clang-tidy`; other available targets: `analyze-cppcheck`, `analyze-iwyu` |
| API docs | `cmake --preset core -DDAGFLOW_BUILD_DOCS=ON && cmake --build --preset core --target docs` | Requires Doxygen/Graphviz |

`distribution` deliberately uses `DAGFLOW_PROJECT_CAPABILITIES=distribution`:
that composes build metadata, reproducible-build path remapping, packaging and
diagnostics. For timestamp-stable artifacts also export `SOURCE_DATE_EPOCH`.
The reproducibility option is opt-in and does not change runtime scheduling.

A local-source vcpkg overlay port can be generated when explicitly requested:

```sh
cmake --preset distribution -DDAGFLOW_GENERATE_VCPKG_PORT=ON
# Generated port files: out/build/distribution/vcpkg-ports/dagflow/
```

This port selects DagFlow's **own** `DAGFLOW_BUILD_SHARED/STATIC` flags according
to `VCPKG_LIBRARY_LINKAGE`, disables optional benches/tests, uses system allocation,
and consumes DagFlow's existing install/export package. It references the current
checkout via an absolute path; it is **not** a remotely reproducible registry
release. A public registry entry requires a pinned source revision/archive plus
verified hashes and its own release checks.

Cross toolchains are in [`toolchains/README.md`](../toolchains/README.md).
SDKs are caller-supplied. `windows-x64-from-linux`, `android-arm64`, and
`wasm32-emscripten` are opt-in; WASM threading/runtime support is experimental.

### Continuous integration coverage

`.github/workflows/dagflow-ci.yml` retains the project-specific tests, ASan,
TSan, fuzz corpus/mutations, dependent-library selection, install and embedded
consumer checks, benchmark smoke, PGO-aware build presets, and host setup/entry
checks. It additionally checks:

- GCC coverage with uploaded LCOV + HTML artifacts;
- real Linux AArch64 cross static library and foreign-architecture external consumer;
- shared/static distribution packages and external static consumers on Linux,
  macOS and Windows, with downloadable package artifacts;
- generated public API docs on Linux.

The special `dagflow-container.yml` workflow is **manual** because it repeats
native tests in an SDK image and consumes extra CI minutes. Locally use:

```sh
bash tools/container.sh test --preset core
bash tools/container.sh package --preset core
bash tools/container.sh shell
```

The Dockerfile has build, test and package stages, but no runtime executable or
ENTRYPOINT: DagFlow is a library. Container packaging installs files under
`/opt/dagflow` and does not run unrelated benchmark/fuzz campaigns.

The CI matrix is purposefully **not** the Cartesian product of compilers,
allocators, sanitizers, platforms, LTO, PGO and benchmarks. The dedicated
campaign presets remain explicit to avoid unstable measurements and excessive
runner usage. Sanitizer and coverage binaries must never be used as performance
baselines.
