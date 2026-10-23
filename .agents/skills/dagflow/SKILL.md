---
name: dagflow
description: Develop, debug, test, or optimize the DagFlow C++ task runtime, including its public APIs, scheduler, ownership, CMake integration, fuzzers, and benchmarks. Use for work in the DagFlow repository; generic DAG workflows and the separate cmake_boilerplate framework are outside this skill.
---

# DagFlow Development

## Locate the change

Find the DagFlow root containing `include/dagflow/dagflow.hpp` and `cmake/DagFlowRuntime.cmake`. Read its `AGENTS.md` and inspect the working-tree diff before editing. Paths below are relative to that root; the skill is stored three directories below it.

Select the relevant contract and implementation:

| Work | Read first | Implementation |
| --- | --- | --- |
| Public APIs and ranges | `docs/api.md`, `docs/batch-submit.md` | `include/dagflow/`, `examples/` |
| Completion, cleanup, graph reuse | `docs/explicit-ownership.md`, `docs/task-scope-lifetime.md` | `handle.hpp`, task graph/scope headers and sources |
| Admission and shutdown | `docs/pool-lifecycle.md` | `src/thread_pool.cpp`, `include/dagflow/thread_pool.hpp` |
| Scheduling and wakeups | `docs/scheduler-topology.md`, `docs/idle-accounting.md`, `docs/how_it_works.md` | `src/scheduler.cpp`, `src/parking_lot.cpp`, `src/idle_accounting.cpp` |
| Build and consumers | `docs/build-and-layout.md`, `cmake/dagflow/UPSTREAM.md` | `cmake/DagFlow*.cmake`, `cmake/dagflow/`, `CMakePresets.json` |

Read only the documents relevant to the requested change; compare documentation with current headers and sources when they disagree.

## Preserve runtime contracts

- Keep storage ownership, packet custody, liveness credits, and observation distinct. A `Handle` observes completion; it does not authorize spawning new credits.
- Before publication, the sender retains RAII ownership. Successful publication transfers packet custody exactly once; failure must release unpublished obligations and preserve accepted work.
- Callable/payload cleanup must complete before the corresponding completion becomes ready. Preserve the distinction between logical completion and physical pool idle accounting.
- Graph runs borrow graph storage under live credits. Do not reuse run storage while an earlier run can still access it. Scope/range failure paths must drain accepted work before borrowed data expires.
- Pool close rejects external admission while accepted descendants can still run; shutdown drains work and joins workers. Check the documented worker-wait policy before changing helping or blocking behavior.
- For queue, counter, or wakeup edits, explain the publication/observation ordering and the race the regression exercises. Passing stress tests alone does not establish correctness.

## Build and validate

Start with the dependency-free configuration when it covers the change:

```sh
cmake --preset core
cmake --build --preset core --parallel 2
ctest --preset core --output-on-failure --no-tests=error
```

Tests are standalone executables using Release-active `CHECK` assertions from `tests/support.hpp`. Register new runtime tests in `cmake/DagFlowTests.cmake`; see `tests/README.md` and `tests/adversarial-tests.md` for existing regressions. Use CTest `-R` or `-L` for focused runs after rebuilding.

Choose additional checks according to the change:

- Lifetime, allocation, or concurrency: configure/build/test `check-asan` or `check-tsan`; `adversarial-tiny` stresses bounded queues. Global allocation-interposition tests have documented TSan/MSVC exclusions.
- Fuzzer changes: follow `fuzz/README.md`. `cmake --build --preset fuzz` only builds; `ctest --preset fuzz --no-tests=error` replays seeds. Run timed campaigns explicitly and retain a minimal reproducer for fixes.
- Build/export changes: exercise the affected configuration and installed consumer tests; preserve `DagFlow::DagFlow` and `DagFlow::DagFlow_static` contracts.
- Performance work: use `docs/benchmarks/runtime-suite.md` or `docs/benchmarks/campaigns.md`. Compare matched compiler, allocator, worker counts, and optimization settings. Instrumented diagnostics are not timing baselines.

DagFlow owns its adapted CMake tree. Port only relevant changes with namespace/behavior review; do not bulk-copy the sibling boilerplate or require it at build time.

Report the changed contract, checks actually run, and remaining validation limits. Update relevant public documentation/examples when observable behavior changes.
