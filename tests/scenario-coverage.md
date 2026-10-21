# Runtime scenario coverage

These checks combine stateful reference models, controlled failure injection
and deterministic concurrency regressions. Branch coverage measures executed
paths; it does not establish that all schedules or lifecycle transitions have
been explored.

| Scenario | Checks and oracle |
| --- | --- |
| Pool detached batch from its worker | [`branch_protocol_property_tests.cpp`](branch_protocol_property_tests.cpp): sizes around the 64-entry boundary, nested helping, priority/affinity, exactly-once execution. |
| Empty and multi-chunk ranges; WS grains | [`branch_closure_property_tests.cpp`](branch_closure_property_tests.cpp), [`range_property_tests.cpp`](range_property_tests.cpp): iterator/span and forward/deque ranges, grain normalization and split boundaries, exact visit counts, errors and cleanup. |
| Publication failure and borrowed lifetime | [`fault_injection_property_tests.cpp`](fault_injection_property_tests.cpp): accepted-prefix custody and recovery. [`range_publication_rollback_lifetime_test.cpp`](range_publication_rollback_lifetime_test.cpp): allocation fails after accepting work; that work is held at a gate, and the exception must not escape before it finishes using borrowed data. Both fixed and WS paths, including worker-side calls, are checked. |
| GraphScope lifecycle | [`graph_property_tests.cpp`](graph_property_tests.cpp): build/run/wait/clear/destruction state model. [`graph_scope_lifecycle_property_tests.cpp`](graph_scope_lifecycle_property_tests.cpp): a parent occupies the only worker so graph work is guaranteed pending before repeated run, clear or destruction; checks implicit/empty wait, failure recording/rethrow, retry, capture cleanup and old result handles. |
| GraphScope dependent ranges | [`remaining_protocol_property_tests.cpp`](remaining_protocol_property_tests.cpp), [`branch_closure_property_tests.cpp`](branch_closure_property_tests.cpp): `parallel_for_after`, iterator/span overloads, empty and multiple chunks, dependent execution and reuse. [`public_api_property_tests.cpp`](public_api_property_tests.cpp): callable construction rollback. |
| TaskGraph options, limits and rollback | [`graph_options_property_tests.cpp`](graph_options_property_tests.cpp): invalid options, lowered node limits, token/capacity policies and recovery. [`graph_builder_property_tests.cpp`](graph_builder_property_tests.cpp): allocator/callable failures, reseal and retained state. [`fault_injection_property_tests.cpp`](fault_injection_property_tests.cpp): lowered node/edge limits. |
| TaskGraph execution model | [`reference_graph_model_property_tests.cpp`](reference_graph_model_property_tests.cpp), [`graph_property_tests.cpp`](graph_property_tests.cpp): independent DAG/token outcomes, dependencies, failures, cancellation and reuse. |
| TaskScope cancellation before callable | [`remaining_protocol_property_tests.cpp`](remaining_protocol_property_tests.cpp): a blocked sole worker keeps children queued; cancellation suppresses their callables, while close permits execution; both paths release captures and finish completion. [`scope_property_tests.cpp`](scope_property_tests.cpp): descendant credits, errors, join/wait and destruction. |
| Handle references and completion | [`completion_property_tests.cpp`](completion_property_tests.cpp), [`completion_failure_reference_property_tests.cpp`](completion_failure_reference_property_tests.cpp): copied/moved/dropped observers, forked credits, combined source sets and first errors. Observer ownership is distinct from liveness credits. |
| Scheduler custody under contention | [`scheduler_property_tests.cpp`](scheduler_property_tests.cpp): custody model. [`scheduler_contention_protocol_test.cpp`](scheduler_contention_protocol_test.cpp): producers and owners race across ingress/overflow/stealing with an exactly-once oracle. This is bounded stress, not exhaustive schedule exploration. |
| Selected adversarial interleavings | [`worker_help_announced_test.cpp`](worker_help_announced_test.cpp): publication between parking announcement and helping. [`graph_cancellation_edge_test.cpp`](graph_cancellation_edge_test.cpp): cancellation during propagation and continuation-budget transitions. These use isolated test-only checkpoints. |

The lifecycle models retain earlier results across later operations; successful
reuse must not change an earlier result's readiness or error. The new pending
GraphScope checks do not rely on a short task happening to remain unfinished.

Limits are intentional: `chunks == 0` after an explicitly positive chunk count
cannot be reached with a valid ordinary range; physical UINT32_MAX-sized graphs
are replaced by controlled limit injection. Queue serial-history checks do not
constitute whole-pool linearizability verification. Sanitizers and adversarial
tests complement PBT; none proves every possible concurrent execution.

## Validation snapshot (2026-10-22)

`cmake --build --preset coverage --target coverage-report --parallel 2`
rebuilt the tests, reset counters, ran all 76 tests successfully (32 labelled
PBT), and generated both raw and exception-filtered LCOV reports.

| Scope | Lines | Branches, `--filter exception` | Raw branches |
| --- | --- | --- | --- |
| `include/dagflow/detail` | 400/400 (100%) | 132/132 (100%) | 169/184 (91.8%) |
| All measured runtime sources | 1882/1882 (100%) | 964/1030 (93.6%) | 1309/1580 (82.8%) |

The 100% figure applies to the measured branches in this GCC Debug/system
allocator configuration with compiler-generated exception paths filtered.
The raw report remains available; this is not 100% raw branch coverage.
LCOV reported two line/branch consistency warnings in `thread_pool.hpp`;
those counts were retained, not excluded to improve the percentage.

Six focused tests passed in a separate Clang Debug ASan/UBSan build: storage,
container, queue, detail vector boundaries, accounting boundaries and GraphScope
lifecycle. LeakSanitizer cannot inspect processes in this environment because
of ptrace restrictions; these runs used `ASAN_OPTIONS=detect_leaks=0`.
