# DagFlow

[![CMake](https://img.shields.io/badge/CMake-3.26+-blue.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![](https://tokei.rs/b1/github/cpp20120/DagFlow)](https://github.com/cpp20120/DagFlow).

DAG-flow runtime.

A minimal runtime for parallel task execution in C++23.
Designed for workloads where you know the dependency graph in advance, need predictable scheduling, and want full control over affinity, priorities, and back-pressure — without the complexity of a full TBB.

Mini-runtime for parallel tasks in C++23:

* Work-stealing pool (bounded Chase–Lev deques, central ring-buffer MPMC queues).

* Bounded ring-buffer MPMC (Vyukov) for central queues — zero per-operation heap allocations.

* Explicit task ownership and move-only completion credits; system allocation by default,
  with mimalloc and tbbmalloc available through CMake.

* Move-only `small_function` with inline storage and runtime-backed spill;
  `inplace_function` for a strict inline-only contract.

* Reusable DAG graphs with per-run token state, bounded admission, and concurrency limits.

* Graph-building API: `emplace`/`then`/`when_all`/`parallel_for` via `GraphScope`.

* Dynamic structured tasks: `TaskScope::spawn`, child spawning through `Context`,
  cooperative cancellation, and joining on destruction.

[Build and repository layout](docs/build-and-layout.md) describes the current
CMake project structure, public includes, targets and consumer contracts.

[Library builds, coverage, cross-compilation, packaging and CI](docs/build-and-layout.md#library-infrastructure-and-ci) document the reusable CMake library framework and new presets.

[Code organization and API migration](docs/code_organization.md) describes the
container contracts and API changes. [Runtime architecture](docs/how_it_works.md)
describes admission, errors, reusable runs, scheduler bypass, and task storage.

[CMake capability validation](docs/cmake-capabilities-validation.md) records the
DagFlow integration checks, remaining gaps, and commands to repeat them.

[Coverage-guided fuzzing](fuzz/README.md) uses instrumented libFuzzer
targets for TaskGraph topology/token/lifetime invariants, Pool publication,
quiescence, shutdown, ParkingLot wakeups, accounting, completion credits,
scheduler custody and fault injection (`cmake --preset fuzz`).

### [Design](https://github.com/cpp20120/DagFlow/blob/main/docs/how_it_works.md)
* Scheduler: local deques (Chase–Lev) + central ring-buffer MPMC shards for external submissions; contiguous Local/Shard arrays; worker drains home ingress and steals within its domain before remote domains.

* Central queues: Vyukov bounded ring buffer (capacity `DAGFLOW_CENTRAL_QUEUE_CAPACITY = 16 384`) — no node allocation per push, no QSBR reclamation needed.

* Local deques: fixed capacity `DAGFLOW_LOCAL_QUEUE_CAPACITY = 1 024` per worker and priority, with atomic slots and no buffer growth. Central batches are capped by available local capacity.

* Queue overflow: a worker tries its local deque and central shard, then publishes to a shared intrusive overflow queue. Submission does not recursively invoke the incoming task. Helping and other workers can acquire overflow tasks; external producers retain bounded-ingress backpressure.
* `Pool::close()` closes external admission; accepted workers can still spawn children. `shutdown()` and pool destruction drain accepted work and join workers. See [the lifecycle contract](docs/pool-lifecycle.md).

* Progress: the central MPMC ring is not strictly lock-free; a producer paused after reserving a slot can delay consumers. Queue operations allocate no memory, but task and completion-counter allocation is a separate concern. The pool as a whole is not a lock-free API.

* Token tracking: per-run execution lanes claim indices; successors open after the last lane completes the node. Compatible successors can bypass the queues.

* Scheduling: shared ingress is probed every 32 acquisitions. `SubmissionMode::Enqueue` lets workers publish independent work there. ParkingLot uses a compact idle bitmap with precomputed shard masks, wakeup epochs and a paired-fence/final-scan handshake.

* Topology: `Config::worker_shards` optionally maps workers to logical domains; affinity hints route through that mapping. See [topology and memory](docs/scheduler-topology.md).

* Idle accounting: single-writer producer/worker counters avoid a pool-wide RMW per task; `wait_idle()` performs the scan. See [contract and lifetime](docs/idle-accounting.md).
* Synchronization: `memory_order` (acquire/release/seq_cst where reconciliation is required).

By default, graph nodes use 128 bytes of inline callable storage. Larger,
over-aligned or potentially throwing-move callables spill through `runtime_memory`.
`inplace_function<Sig, N, Align>` rejects targets that cannot remain inline.

For "heavy" stages, set `concurrency > 1` in `ScheduleOptions`/`NodeOptions`.

Configure `Config` for CPU/NUMA; `pin_threads = true` is useful for cache stability.

Node capacity limits admitted live executions. `Block` defers excess tokens,
`Drop` admits at most capacity tokens, and `Fail` cancels an overflowing run.
This finite-DAG policy is not a streaming pipeline contract. Pool waits observe
completion; call `handle.rethrow_if_failed()` to propagate task errors.

The [implementation plan](docs/runtime-improvement-plan.md) records completed work
and deferred experiments. [Earlier runtime measurements](docs/runtime-benchmark-results.md)
predate the current ownership and allocator changes. See
[explicit ownership](docs/explicit-ownership.md) for the current lifetime rules.

The [runtime benchmark suite](docs/benchmarks/runtime-suite.md) covers dynamic
scopes, DAG reuse, queue saturation, stealing and idle bursts, with separate
throughput/latency passes and Release/LTO/PGO matrices.
The [`main.cpp` stress harness](docs/benchmarks/main-harness.md) adds independent
producer/worker controls, payload verification, phase-gated perf counters,
callgraphs/flamegraphs and separate runtime path diagnostics. Run its O3/LTO
matrix with the [CMake campaign targets](docs/benchmarks/campaigns.md).

### Latest benchmark run

The old table used an earlier runtime and is no longer a useful baseline. The
current public-API run was measured on 2026-10-02 with an AMD Ryzen 7 6800H,
Clang 22.1.8, `-O3 -g -DNDEBUG`, system allocation, eight workers, five measured
runs and one warmup. Native CPU flags and LTO were disabled. The table compares
the same workload and timing boundaries with oneTBB 2023.1.0; lower DagFlow time
is better.

| Benchmark | DagFlow mean | oneTBB mean | DagFlow throughput | oneTBB throughput |
|---|---:|---:|---:|---:|
| Dependent chain (1,000 tasks) | 298.0 µs | 129.9 µs | 3.356 M task/s | 7.699 M task/s |
| Independent tasks (1,000) | 2.733 s | 2.574 s | 365.9 task/s | 388.4 task/s |
| Independent batched (1,000, b=10) | 2.748 s | — | 363.8 task/s | — |
| Parallel_for (1,000,000 elements) | 43.024 ms | 37.341 ms | 23.243 M elem/s | 26.780 M elem/s |
| Workflow (width=10, depth=5) | 55.4 µs | 37.5 µs | 903.267 k task/s | 1.335 M task/s |
| Noop tasks (1,000,000) | 457.295 ms | 69.879 ms | 2.187 M task/s | 14.310 M task/s |

These numbers are one measurement on one machine, not performance guarantees.
The runtime matrix with DAG reuse, stealing, overflow, idle bursts and latency
quantiles is documented in [the runtime benchmark suite](docs/benchmarks/runtime-suite.md).
The CMake and stress-harness reports are described in [the benchmark workflow](docs/benchmarks/cmake.md).

### Build and usage

DagFlow keeps its CMake build infrastructure under `cmake/dagflow/`.
`Bootstrap.cmake` initializes the project and `DagFlow.cmake` provides the
project, target and profile helpers. The adjacent DagFlow modules describe the
runtime's sources, allocator and workloads. Build settings and component options use `DAGFLOW_*`.

Builds, tests, PGO, documentation and benchmark campaigns run without Python.
See [CMake campaigns and migration](docs/benchmarks/campaigns.md) for commands,
allocator/profile matrices and result comparison. Only the optional specialized
perf/flamegraph tools still use Python.

After cloning, prepare the tools, build, run CTest and execute the basic example:

```sh
./setup.sh --run                     # Linux / macOS
./build.sh --run                     # subsequent builds
```

On Windows, from PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\setup.ps1 -Run
powershell -NoProfile -ExecutionPolicy Bypass -File .\build.ps1 -Run
```

Setup checks existing tools and reuses them, installs missing prerequisites,
then calls the separate build script. `--setup-only` (`-SetupOnly`) only prepares
tools; `--dry-run` (`-DryRun`) previews the commands. First installation needs
network access and may require OS administrator authorization. See
[setup profiles and packaging](docs/build-and-layout.md#host-setup-and-build-entry-points).

For manual CMake commands below, supply CMake 3.26+, a C++23 compiler and its
native build tools. The presets use CMake's default compiler/generator;
Ninja and Clang are optional for the core build.

System allocation is the default, including plain `cmake -S . -B build`.
Selecting `DAGFLOW_BUILD_BENCH=ON`, `DAGFLOW_ALLOCATOR=mimalloc` or
`DAGFLOW_ALLOCATOR=tbbmalloc` automatically selects the matching vcpkg manifest
features. If no explicit vcpkg root/toolchain is supplied, CMake provisions a
pinned vcpkg checkout (shared under `out/host-tools/` by the build scripts).
The first dependency build
needs Git and network access; subsequent builds reuse the checkout and packages.
No `TBB_DIR`, global package installation or manual vcpkg bootstrap is needed.

```sh
git clone https://github.com/cpp20120/DagFlow.git
cd DagFlow
cmake --preset core
cmake --build --preset core
ctest --preset core
```

Choose `runtime-check` for dependency-free runtime benchmark smoke tests,
`bench-check` for the full C++ benchmark set including oneTBB and correctness
tests, or `mimalloc` / `tbbmalloc` for allocator builds. Each has matching
configure, build and test presets. `bench-check-vcpkg` explicitly names the
same automatic dependency path. `clang` and `gcc` select a compiler with Ninja.
For example, `./setup.sh --preset bench-check` prepares tools and runs the full
benchmark smoke checks; on Windows use `setup.ps1 -Preset bench-check`.

Presets compose the framework's target policies and profiles. The main switches
are `DAGFLOW_BUILD_SHARED`, `DAGFLOW_BUILD_STATIC`, `DAGFLOW_BUILD_TESTS`,
`DAGFLOW_BUILD_EXAMPLES`, `DAGFLOW_INSTALL`, `DAGFLOW_ALLOCATOR` and
`DAGFLOW_COMPILER_CACHE`. Use `DAGFLOW_PROFILE=lto` for an explicit profile or
`DAGFLOW_PGO_MODE=generate|use` for PGO. The install tree exports
`DagFlow::DagFlow` and `DagFlow::DagFlow_static` for `find_package` consumers.
[GitHub Actions](.github/workflows/dagflow-ci.yml) defines fresh-checkout core,
runtime, oneTBB and allocator jobs on Linux, Windows and macOS, plus Linux
sanitizer/fuzz jobs. See [dependency and toolchain controls](docs/build-and-layout.md)
for offline/system builds and compiler-specific profiles.

Build the complete benchmark set through CMake:

```sh
cmake --preset bench-all
cmake --build --preset bench-all --target dagflow_benchmarks --parallel 4
```

The individual targets are `dagflow_runtime_bench`, `dagflow_runtime_suite`,
`dagflow_stress_bench`, `dagflow_public_api_bench`, `dagflow_tbb_bench` and
`dagflow_function_bench`. Each has a matching `dagflow_run_*` target for an
explicit CMake-managed run. `bench-check` is the short correctness and smoke
matrix; `bench-lto`, `bench-full-lto`, `bench-pgo-generate` and `bench-pgo-use`
cover the optimized profiles.

For a standalone PGO cycle without Python:

```sh
cmake --preset bench-pgo-generate
cmake --build --preset bench-pgo-generate --target dagflow_pgo_merge --parallel 4
cmake --preset bench-pgo-use
cmake --build --preset bench-pgo-use --target dagflow_benchmarks --parallel 4
```

Queue and scheduler regression tests:

See the [test suite guide](tests/README.md) for coverage, a build without
external allocator dependencies, test selection, and sanitizer configurations.

```sh
cmake --preset bench-check
cmake --build --preset bench-check
ctest --preset bench-check
```

Tests cover last-element owner/thief races, ring-slot reuse, delayed MPMC
publication, queue saturation, oversized batches, nested waits after stealing,
cross-pool submissions, container lifetimes, DAG dependencies, chunked algorithms,
and allocation/deallocation counts for queue operations
(allocation interception test on non-MSVC builds).

How to use in your CMake project

1. Via `add_subdirectory`:
```cmake
add_subdirectory(external/DagFlow)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE DagFlow::DagFlow)
```

2. Via `find_package`:
```cmake
find_package(DagFlow REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE DagFlow::DagFlow)
```

`find_package` works after `cmake --install`.

2.5 On Windows, add to your target:
```cmake
if (WIN32 AND DAGFLOW_BUILD_SHARED)
  add_custom_command(TARGET ${CMAKE_PROJECT_NAME} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      $<TARGET_FILE:DagFlow>
      $<TARGET_FILE_DIR:my_app>
  )
endif()
```

### Examples and everyday API

[Runnable examples](examples/README.md) cover
individual tasks, structured spawning, cancellation, reusable graphs, ranges,
and detached batch submission.

```cpp
#include <dagflow/dagflow.hpp>
#include <vector>

dagflow::Pool pool;
std::vector<int> data(1000, 1);
auto done = pool.for_each(data, [](int& x) { x *= 2; });
pool.wait_and_rethrow(done);
```

`wait_and_rethrow()` waits and propagates task errors; `wait()` still only waits.
Range overloads accept lvalue containers and borrowed temporaries such as
`std::span(data)`, and reject owning temporaries such as `std::vector<int>(1000)`.
Keep the backing data alive and its iterators valid until completion; graph
algorithms retain those iterators for subsequent runs. Pool range algorithms
share their callable across tasks, so concurrent callback calls must be safe.

Minimal example
```cpp
#include <dagflow/dagflow.hpp>

dagflow::Pool pool;
dagflow::GraphScope scope(pool);

auto a = scope.emplace([] { /* … */ });
auto b = scope.then(a, [] { /* … */ });
auto c = scope.when_all({a, b}, [] { /* … */ });

scope.run_and_wait();
```

Dynamic tasks use a separate one-shot scope:

```cpp
dagflow::TaskScope scope(pool);
scope.spawn([](dagflow::TaskScope::Context& ctx) {
  ctx.spawn([] { /* child work */ });
});
scope.join(); // closes external admission, waits for descendants, rethrows errors
```

`close()` stops external submission while live children retain spawning rights.
`cancel()` also stops child spawning and skips callbacks that have not passed
their cancellation check; running callbacks finish cooperatively. `wait()` and
destruction drain without rethrowing task errors. The pool must outlive its
scopes. See [TaskScope lifetime and admission](docs/task-scope-lifetime.md).
