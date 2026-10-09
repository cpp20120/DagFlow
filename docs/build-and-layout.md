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

Use a separate binary tree and Ninja; system allocation avoids optional
mimalloc/oneTBB dependencies.

```sh
cmake -S . -B out/build/dev -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DDAGFLOW_ALLOCATOR=system -DDAGFLOW_BUILD_SHARED=OFF \
  -DDAGFLOW_BUILD_STATIC=ON -DDAGFLOW_BUILD_TESTS=ON \
  -DDAGFLOW_BUILD_EXAMPLES=ON -DDAGFLOW_BUILD_STRESS_BENCH=ON \
  -DDAGFLOW_BUILD_RUNTIME_BENCH=ON
cmake --build out/build/dev --parallel
ctest --test-dir out/build/dev --output-on-failure
```

`DAGFLOW_BUILD_RUNTIME_BENCH` also selects the scenario suite by default.
For the preconfigured compiler/profile matrices use `cmake --list-presets`,
`cmake --preset <name>`, `cmake --build --preset <name>` and
`ctest --preset <name>`.

To build all optional C++ benchmark programs, including oneTBB comparison,
configure `bench-all` with oneTBB installed. `dagflow_benchmarks` builds
without executing them. To run a particular benchmark explicitly, build its
`dagflow_run_*` target. See [CMake campaigns](benchmarks/campaigns.md) for
cases, JSON results, comparison and PGO. Normal builds do not execute
benchmark campaigns or invoke Python.

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

`DAGFLOW_ALLOCATOR` is `mimalloc` (default), `tbbmalloc` or `system`.
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
