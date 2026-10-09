# DagFlow tests

The ordinary test suite checks runtime correctness, ownership, allocation
failures, public APIs, and build integration. Tests are registered with CTest by
[`cmake/DagFlowTests.cmake`](../cmake/DagFlowTests.cmake) when both
`DAGFLOW_BUILD_TESTS` and `BUILD_TESTING` are enabled. Most runtime tests also
require a shared or static DagFlow library target.

Each C++ test is a standalone executable. The `CHECK` macro in
[`support.hpp`](support.hpp) prints the failed expression and source location
before aborting; checks remain active in Release builds. Runtime unit tests have
a 60-second CTest timeout. A passing concurrency test samples thread schedules;
it does not prove the absence of every race.

The thirteen targeted lifetime/race regressions are documented separately in
[`adversarial-tests.md`](adversarial-tests.md). Coverage-guided harnesses and
their input corpora are described in [`fuzz/README.md`](../fuzz/README.md).
Ordinary tests do not need libFuzzer, Python, or a seed corpus.

## Build and run

Run these commands from the repository root. A fresh build directory with
CMake 3.26+, Ninja, and a C++23 compiler is sufficient; the system allocator
avoids external allocator dependencies. This configuration includes runtime
tests, adversarial regressions, and executable examples:

```sh
cmake -S . -B out/build/tests -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DDAGFLOW_ALLOCATOR=system \
  -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_BUILD_STATIC=ON \
  -DDAGFLOW_BUILD_TESTS=ON -DBUILD_TESTING=ON \
  -DDAGFLOW_BUILD_EXAMPLES=ON
cmake --build out/build/tests --parallel 4
ctest --test-dir out/build/tests --output-on-failure --no-tests=error
```

Benchmark programs and fuzzers default to disabled in this fresh configuration.
CTest runs existing binaries; rebuild after changing sources. For generators
with multiple configurations, pass `--config Debug` to the build and `-C Debug`
to CTest. Keep different compilers and sanitizer modes in separate build trees.

## Select tests

CTest names match C++ target names, including the `dagflow_` prefix. List the
registered tests or build and run one executable:

```sh
ctest --test-dir out/build/tests -N
cmake --build out/build/tests --target dagflow_queue_tests --parallel 4
ctest --test-dir out/build/tests -R '^dagflow_queue_tests$' --output-on-failure --no-tests=error
```

Labels select groups; their availability depends on the enabled components:

| Label | Tests selected |
| --- | --- |
| `unit` | Ordinary C++ runtime and allocation tests; also the Linux perf-control test when enabled. |
| `adversarial` / `runtime` | The thirteen lifetime/race regressions. These use both labels and do not carry `unit`. |
| `benchmark` | Benchmark smoke checks, JSON/CLI checks, and Linux perf-control checks. |
| `schema` | Stress-harness and runtime-suite JSON/CLI checks. |
| `build` | CMake configuration regressions and the campaign test. |
| `harness` | Campaign output and comparison checks. |

Example executables are registered as `dagflow_example_*`; select them with
`-R '^dagflow_example_'`. They have no label in the DagFlow adapter.

```sh
ctest --test-dir out/build/tests -L unit --output-on-failure --no-tests=error
ctest --test-dir out/build/tests -L adversarial --output-on-failure --no-tests=error
ctest --test-dir out/build/tests -R '^dagflow_queue_tests$' --repeat until-fail:20 --output-on-failure
```

Use `-V` for successful-test output too. Failure logs are saved under the build
directory's `Testing/Temporary/LastTest.log`. Repeating a race test samples more
interleavings; it does not replace sanitizer runs.

## Runtime coverage

The table omits the common `dagflow_` target prefix. Each name links to its
source; `function_tests.cpp` is compiled both against the runtime and against a
recording allocation backend.

| Target suffix | Checks |
| --- | --- |
| [`queue_tests`](queue_tests.cpp) | Chase–Lev boundaries, last-element owner/thief races, wraparound, MPMC contention, and delayed slot publication. |
| [`container_tests`](container_tests.cpp) | Inline/heap vector storage, alignment, moves, exception cleanup, queue ownership transfer, and callable lifetime/arguments. |
| [`function_tests`](function_tests.cpp) | Callable storage boundaries, over-alignment, inline/spilled lifetime, move ownership, signatures, and throwing construction. |
| [`pool_queue_tests`](pool_queue_tests.cpp) | Worker overflow, external backpressure, cross-pool/concurrent submission, stolen-batch dependencies, range failures, completion edges, and handles surviving the pool. |
| [`pool_lifecycle_tests`](pool_lifecycle_tests.cpp) | Draining destruction, close versus producers, descendants after close, shutdown during backpressure, capture cleanup, and nonrecursive overflow. |
| [`pool_memory_tests`](pool_memory_tests.cpp) | External-task capture lifetime and shared-ingress progress while worker-local tasks keep refilling queues. |
| [`idle_accounting_tests`](idle_accounting_tests.cpp) | Pending descendants, multiple waiters, publication during capture destruction, reused pool addresses, TLS eviction, and no premature idle result while work changes owners. |
| [`scheduler_topology_tests`](scheduler_topology_tests.cpp) | Sparse shard membership, stealing, transferred-batch visibility, worker recruitment, parking/publication races, packed bitmap boundaries, and shared-source service. |
| [`scheduler_fairness_tests`](scheduler_fairness_tests.cpp) | External progress under local load, shard rotation, and high-priority probing. |
| [`batch_submit_tests`](batch_submit_tests.cpp) | Move-only batches, descendants, overflow, concurrent producers, backpressure, accepted-prefix failure, and task exceptions. |
| [`ownership_tests`](ownership_tests.cpp) | Completion-credit transfer, payload cleanup, observer release, dependent-registration races, graph storage reuse after readiness, and spilled-callable ownership. |
| [`api_convenience_tests`](api_convenience_tests.cpp) | Wait/rethrow helpers, range overload constraints, pool range algorithms, and graph ranges. |
| [`task_scope_tests`](task_scope_tests.cpp) | Admission, descendants after close, cancellation, first-error propagation, cooperative/self-join, draining destruction, and publication reservations racing close/cancel. |
| [`graph_tests`](graph_tests.cpp) | Dependencies, tokens, capacity policies, errors, cancellation/retry, graph reuse/recompilation, GraphScope ranges, lane visibility, and priority progress. |
| [`scaling_tests`](scaling_tests.cpp) | Correctness across containers, queues, scheduling, completion, parking, and public APIs with configurable worker count, including shutdown with overflow. Printed durations are diagnostic, not performance thresholds. |

## Allocation and failure coverage

| Target suffix | Checks |
| --- | --- |
| [`runtime_memory_tests`](runtime_memory_tests.cpp) | Array storage, alignment, freeing on another thread, and zero-size allocation through the selected runtime allocator. |
| [`stl_allocation_tests`](stl_allocation_tests.cpp) | STL allocator routing, rebind/alignment, moves/swaps, construction failures, overflow rejection, and balanced allocation lifetimes. |
| [`vector_allocation_tests`](vector_allocation_tests.cpp) | Inline capacity, heap growth/reserve, storage stealing, alignment, and rollback during allocation or element relocation failures. |
| [`function_allocation_tests`](function_tests.cpp) | Callable allocation counts and failure cleanup using `DAGFLOW_FUNCTION_TEST_BACKEND`. |
| [`queue_allocation_tests`](queue_allocation_tests.cpp) | Global allocation interception verifies allocation-free queue operations. |
| [`topology_allocation_tests`](topology_allocation_tests.cpp) | Scheduler/parking storage allocation counts across worker counts and cleanup at construction-failure cutoffs. |
| [`accounting_allocation_tests`](accounting_allocation_tests.cpp) | Accounting construction/producer-registration failures, no phantom publication, and reuse of registered lanes after TLS eviction. |
| [`runtime_failure_tests`](runtime_failure_tests.cpp) | Allocation failure during pool construction, graph building/sealing/continuations, scope publication, and batch publication; graph runtime storage reuse. |

Tests with a recording backend replace the `runtime_memory` allocation boundary
so requests and live allocations can be checked directly. The queue-allocation
and runtime-failure tests replace global `new`/`delete`; these two are omitted
on MSVC and under ThreadSanitizer. `runtime_failure_tests` additionally requires
`DAGFLOW_ALLOCATOR=system`. The adversarial allocation-failure tests use a private
runtime backend and remain available under TSan.

## Benchmark and build integration

Enabling benchmark targets adds short correctness checks to CTest. The
`bench-check` preset includes the oneTBB comparison and therefore requires
oneTBB, as well as the preset's Clang/Ninja toolchain:

```sh
cmake --preset bench-check
cmake --build --preset bench-check --parallel 4
ctest --preset bench-check --no-tests=error
```

To enable DagFlow's benchmark checks without oneTBB, extend the earlier build:

```sh
cmake -S . -B out/build/tests \
  -DDAGFLOW_BUILD_RUNTIME_BENCH=ON -DDAGFLOW_BUILD_RUNTIME_SUITE=ON \
  -DDAGFLOW_BUILD_STRESS_BENCH=ON -DDAGFLOW_BUILD_PUBLIC_API_BENCH=ON
cmake --build out/build/tests --parallel 4
ctest --test-dir out/build/tests --output-on-failure --no-tests=error
```

| Test or fixture | Checks and availability |
| --- | --- |
| `dagflow_*_smoke` | Short runs of enabled runtime, runtime-suite, stress, public-API, and oneTBB benchmarks. |
| [`dagflow_main_harness_tests`](main_harness_tests.cmake) | Stress scenarios, exact task counts/checksums, JSON timestamps, sample accounting, duration limits, and invalid CLI arguments; requires the stress benchmark. |
| [`dagflow_benchmark_suite_tests`](benchmark_suite_tests.cmake) | Runtime scenario enumeration, JSON schema, percentile ordering, latency sampling, batching/shards, checksums, and invalid arguments; requires the runtime suite. |
| [`dagflow_perf_control_tests`](perf_control_tests.cpp) | Enable/disable sequencing and both acknowledgement formats through pipes; requires Linux and the stress benchmark, but no live perf recording. |
| [`dagflow_campaign_tests`](campaign_tests.cmake) | Campaign summary/manifest generation and rejection of synthetic slowdowns, mismatched arguments, and failed results; requires the runtime suite. |
| [`dagflow_cmake_configuration_tests`](cmake_configuration_tests.cmake) | Optional dependency handling, invalid settings, PGO dependencies for late-added sources, independent tool lookup, capability registration, and analysis source filtering; registered with Ninja and non-MSVC Clang. |
| [`install_consumer/`](install_consumer/CMakeLists.txt) | Separate consumer project validates installed package discovery and preferred/shared/static exported targets. CI installs, builds, and runs it separately from the main CTest suite. |
| [`cmake_capabilities/`](cmake_capabilities/CMakeLists.txt) | Separate project exercises embedded toolkit capabilities, metadata, fuzzing, and optional RapidCheck/CUDA fixtures; see [capability validation](../docs/cmake-capabilities-validation.md). |

These checks validate benchmark behavior and result formats. They do not impose
a machine-specific throughput baseline. Ordinary CTest registration does not
automatically run the separate consumer and capability projects.

## Sanitizers and small queues

For the Clang/Ninja benchmark matrix, use separate ASan/UBSan and TSan presets
(these inherit the oneTBB dependency from `bench-check`):

```sh
cmake --preset check-asan
cmake --build --preset check-asan --parallel 4
ctest --preset check-asan --no-tests=error

cmake --preset check-tsan
cmake --build --preset check-tsan --parallel 4
ctest --preset check-tsan --no-tests=error
```

For a runtime-only sanitizer build, reuse the explicit options from the first
configure command in a fresh directory, select `-DCMAKE_CXX_COMPILER=clang++`,
and add `-DBOILERPLATE_SANITIZER=address-undefined` or
`-DBOILERPLATE_SANITIZER=thread`. Build and run CTest in that directory.

`adversarial-tiny` builds and runs only the thirteen adversarial regressions
with two-slot queues and a central batch of one. The ordinary tests use normal
queue capacities. See [adversarial test instructions](adversarial-tests.md) for
the tiny-queue commands and detailed lifetime invariants.
