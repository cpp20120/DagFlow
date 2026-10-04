# DagFlow API

DagFlow is a C++23 work-stealing runtime with task scopes and reusable DAGs.

The generated API reference is organized around the public entry points:

- [Thread pool and submission](../include/dagflow/thread_pool.hpp)
- [Structured task scopes](../include/dagflow/task_scope.hpp)
- [Reusable task graphs](../include/dagflow/task_graph.hpp)
- [Graph convenience API](../include/dagflow/graph_scope.hpp)
- [Completion handles](../include/dagflow/handle.hpp)

The design and lifetime contracts are documented in [how it works](how_it_works.md),
[pool lifecycle](pool-lifecycle.md), and [task scope lifetime](task-scope-lifetime.md).
