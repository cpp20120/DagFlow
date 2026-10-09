# Build and repository layout

DagFlow is a C++23 library. CMake 3.26+ configures the project; the reusable
framework is vendored at `cmake/boilerplate/` from the standalone upstream
`cmake_boilerplate/lib/cmake` tree. DagFlow-specific adapters remain in
`cmake/DagFlow*.cmake` and are not copied into the general-purpose toolkit.

```text
include/dagflow/         installed public headers and implementation-detail headers
src/                     library translation units only
examples/                minimal executable API examples
bench/                   stress harness, runtime cases and benchmarks
tests/                   runtime unit, schema, regression and packaging tests
cmake/DagFlow*.cmake     DagFlow project glue and benchmark campaigns
cmake/boilerplate/       vendored generic CMake library toolkit
tools/profiling/         optional Python Linux-perf analysis only
docs/                    design notes and historical measurements
```

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

- `BOILERPLATE_DEPENDENCY_PROVIDER=auto` is DagFlow's default: system for core,
  vcpkg when an external dependency is requested. Direct `-D` options and presets
  use the same selection. Explicit `VCPKG_MANIFEST_FEATURES` are preserved.
- `BOILERPLATE_DEPENDENCY_PROVIDER=system` uses packages already supplied by the
  caller (including their toolchain/prefix); it never downloads vcpkg.
- Explicit `CMAKE_TOOLCHAIN_FILE`, `BOILERPLATE_VCPKG_ROOT` and `VCPKG_ROOT` take
  priority. With no override, `BOILERPLATE_VCPKG_BOOTSTRAP=ON` provisions
  `<build>/_deps/vcpkg-<commit>` and the official toolchain installs into
  `<build>/vcpkg_installed`. A bad explicit root is an error.
- `BOILERPLATE_VCPKG_BOOTSTRAP=OFF` restores the framework's discovery-only
  behavior. `BOILERPLATE_VCPKG_CACHE` shares managed checkouts between builds;
  `BOILERPLATE_VCPKG_REPOSITORY` accepts a Git mirror. Package binary/download
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
oneTBB comparison target. `BOILERPLATE_PROFILE`,
`BOILERPLATE_SANITIZER`, and `BOILERPLATE_PGO_MODE` belong to the framework;
`DAGFLOW_*` options toggle DagFlow components.

PGO uses explicit `boilerplate_pgo_train` / `boilerplate_pgo_merge` steps;
profile generation and use are separate builds. Documentation uses
`DAGFLOW_BUILD_DOCS=ON` and target `docs`, subject to Doxygen availability.
The optional `tools/profiling/*.py` utilities perform specialized Linux
`perf`/flamegraph analysis and are not dependencies of CMake/CTest.

## Fuzzing

`cmake --preset fuzz`, `cmake --build --preset fuzz`, and `ctest --preset fuzz`
exercise eighteen active layers through the boilerplate fuzz API. The private
fuzz runtime receives coverage and sanitizer instrumentation without changing
normal library targets. See [fuzz layers and campaigns](../fuzz/README.md).

The thirteen [adversarial runtime regressions](../tests/adversarial-tests.md)
are ordinary CTest tests under the `adversarial` label. Build them together with
target `dagflow_adversarial_tests`; preset `adversarial-tiny` runs the same group
with two-slot queues and a central batch of one.
