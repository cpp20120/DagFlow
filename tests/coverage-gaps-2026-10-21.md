# LCOV gap audit — 2026-10-21

Baseline: user-supplied `html.zip` (`filtered.info`, 1,760 / 1,799
instrumented lines, or 97.8%). This inventory lists **source lines**, not
function-template specialization counts or branch/memory-order coverage.

A new state-machine executable,
[`coverage_boundary_property_tests.cpp`](coverage_boundary_property_tests.cpp),
was added without removing or changing existing unit tests. It checks concrete
contracts and reproduces the previously unexecuted **17 lines** below under
GCC `--coverage`, Linux, and `DAGFLOW_ALLOCATOR=system`:

| Paths, original LCOV lines | Covered by new stateful PBT |
| --- | --- |
| `src/thread_pool.cpp:98,108-110,114` | Configured worker pinning is best-effort; workers still make progress. Linux-only lines. |
| `src/thread_pool.cpp:235-237` | Raw `Fn,void*` admission, result ownership, close/reopen and exception propagation. |
| `src/thread_pool.cpp:580` | A worker receives `logic_error` for `wait_idle` on its own pool, without wedging the pool. |
| `include/dagflow/thread_pool.hpp:141-143` | Throwing callable move during packet construction rolls back storage and completion credits. |
| `include/dagflow/graph_scope.hpp:216-217` | Dirty cyclic scope tries to run on destruction, catches validation error, retires captures. |
| `include/dagflow/detail/small_vector.hpp:127` | Mutable indexed access also traverses inline/heap growth, checked against vector model. |
| `src/runtime_memory.cpp:72,78` | Backend reporting is invoked and validated; `system` return is build-specific. |

**Not coverable through feasible, defined public API sequences** (22 baseline
lines):

| Paths, original LCOV lines | Reason / required strategy |
| --- | --- |
| `include/dagflow/detail/idle_accounting.hpp:54`, `src/idle_accounting.cpp:39-40` | Requires exhausting a `uint64_t` publication lane. Test-only counter injection would be necessary. |
| `src/idle_accounting.cpp:111-114` | `std::condition_variable::wait()` throwing with otherwise valid synchronization state is not a deterministic public API operation; requires an injected wait backend. |
| `src/parking_lot.cpp:103-107` | Likewise requires the underlying timed condition-variable wait to throw. Spurious wakes/timeouts do **not** throw. |
| `include/dagflow/task_graph.hpp:165`, `src/task_graph.cpp:15` | Requires ~4.29 billion nodes/edges respectively; impractical in an ordinary property trace. |
| `src/task_graph.cpp:65-66` | Defensive duplicate validation: `TaskGraph::emplace_indexed()` rejects `Block` with capacity zero **before** a node enters the builder, so normal `seal()` cannot see it. |
| `src/task_scope.cpp:63` | Requires saturating the concurrent publisher counter; no feasible bounded trace. |
| `src/thread_pool.cpp:192` | Requires saturating the admission reservation counter; no feasible bounded trace. |
| `src/thread_pool.cpp:329,332-334` | Partial batch publication's catch requires `publish_external()` to fail after a previously accepted prefix; an already registered producer lane does not allocate per item. Deterministic injection at this publication boundary would be needed. |

A separate **fault-injection build** with explicit test-only seams is the honest
way to test those exceptional branches; random command generation cannot
manufacture them. Passing them by modifying production state through undefined
behavior or corrupting private counters would not demonstrate runtime
correctness.

## How to run

```sh
cmake --preset coverage
cmake --build --preset coverage --target coverage-report --parallel 4
xdg-open out/build/coverage/coverage/html/index.html

# A focused replay is available if a machine fails:
DAGFLOW_PROPERTY_MODEL='raw admission, pinning and worker idle policy' \
DAGFLOW_PROPERTY_TRACE='0,0,0;2,0,0;4,0,0;0,0,0;5,0,0' \
  out/build/coverage/dagflow_coverage_boundary_property_tests
```

GCC-based per-object `.gcov` inspection verified execution of the 17 newly
addressed lines in the isolated test build. This is **not** a claim that a new
full-suite LCOV report has measured exactly `98.7%`: the percentage depends on
all enabled targets, compiler flags, code-generation and template instances.
Branch coverage, race/interleaving coverage and protocol transition coverage
are separate measures.

## Follow-up: controlled exceptional-path tests

A separate test runtime now covers the previously listed exceptional branches
under deterministic fault injection without altering the production target.
See [`fault-injection-coverage.md`](fault-injection-coverage.md) for the mapping,
limitations, and runnable CTest target. The table above continues to describe
what is **not reachable through ordinary public API operation sequences**.
