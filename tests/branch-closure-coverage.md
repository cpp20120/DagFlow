# Branch-coverage closure suite 
## Targeted paths and independent oracles

| Target | New test | Verified behavior |
| --- | --- | --- |
| `thread_pool.hpp:240,304,350` | `branch_closure_property_tests` | Zero/nonzero ranges, fixed >16K chunks, WS split/one-tile cases, borrowed `span` and iterators; one invocation per element |
| `graph_scope.hpp:108,132,214` | `branch_closure_property_tests` | Empty and multi-chunk `parallel_for_after`, destructor auto-run, idempotent run, replay, clearing and complete predecessor-to-successor ordering |
| `task_graph.cpp:39` | `branch_closure_property_tests` | Mutating/re-running an executing graph is rejected, failed or cancelled runs remain drainable |
| `small_vector.hpp:154` | `branch_closure_property_tests` | `reserve(max_size()+1)` throws before allocation; vector stays usable after rejection |
| `scheduler.cpp:238` | `scheduler_targeted_paths_test` | Periodic external high/normal poll on acquisition 32 |
| `scheduler.cpp:261` | `scheduler_targeted_paths_test` | Same-shard owner queue stolen by another worker; batched siblings have exactly one consumer |
| `thread_pool.cpp:575` | `worker_help_announced_test` | A publish after `prepare()` is picked up by `help(true)` rather than a lost wake/park; a single worker finishes the credit |
| `task_graph.cpp:348` | `graph_cancellation_edge_test` | Cancel arrives after entering successor traversal, before an edge is activated |
| `task_graph.cpp:369` (now 371) | `graph_cancellation_edge_test` | Cancel is observed after 32 inline jumps at the cooperative 64-unit budget boundary |

The tests use the normal `property_support.hpp` generator and replay protocol
for stateful models, and bounded control points for the narrow concurrent
windows. Runtime production behavior remains unchanged. The new graph hooks
in `src/task_graph.cpp` compile out unless `DAGFLOW_FUZZ_HOOKS` is set;
this flag is enabled only for `dagflow_test_fault_runtime`.

## Commands

```sh
cmake --preset coverage
cmake --build --preset coverage --target \
    dagflow_model_property_tests dagflow_scheduler_targeted_paths_test \
    dagflow_worker_help_announced_test dagflow_graph_cancellation_edge_test \
    --parallel 2
ctest --preset coverage -L pbt --output-on-failure
ctest --preset coverage -R 'dagflow_(scheduler_targeted_paths|worker_help_announced|graph_cancellation_edge)_test' --output-on-failure
cmake --build --preset coverage --target coverage-report --parallel 2
```

