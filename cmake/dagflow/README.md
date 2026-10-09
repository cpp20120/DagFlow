# DagFlow CMake infrastructure

Include `Bootstrap.cmake` before `project()` and `DagFlow.cmake` after it.
Settings use `DAGFLOW_*`; functions and workflow targets use `dagflow_*`.
Presets live in the repository's `CMakePresets.json`.

| Directory | Responsibility |
| --- | --- |
| `build/` | Compiler checks, profiles, target policies and preset orchestration |
| `testing/` | CTest, property tests, fuzzing, coverage and workload registration |
| `benchmark/` | Process harness, scenarios, result comparison and PGO |
| `packaging/` | Shared/static libraries, install/export, vcpkg ports and native packages |
| `dependencies/` | Dependency providers and vcpkg discovery/bootstrap |
| `platforms/` | Runtime policies and host/target execution |
| `project/` | Lifecycle, developer tools, documentation and build metadata |

Runtime sources and DagFlow-specific workload definitions are in the parent
`cmake/DagFlow*.cmake` files. GPU, UI, shader and application-deployment modules
are not part of this tree. See [provenance](UPSTREAM.md) for its origin.
