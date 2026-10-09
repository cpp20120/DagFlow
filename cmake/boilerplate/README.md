# CMake module layout

Copy this entire directory to reuse the library toolkit. The two entry points stay
at the root: include `Bootstrap.cmake` before `project()`, then `Boilerplate.cmake`
after it. Public `boilerplate_*` functions and preset names are unchanged.

| Directory | Responsibility |
| --- | --- |
| `build/` | Build profiles, target policies/options, artifact roles, control-flow checks and preset orchestration |
| `testing/` | CTest, property tests, fuzzing, coverage and workload target registration |
| `benchmark/` | CMake process harness, scenarios, result comparison and PGO execution/profile merging |
| `packaging/` | Library variants, install/export templates, vcpkg ports, CPack and runtime delivery helpers |
| `dependencies/` | Dependency providers, allocator adapters and vcpkg discovery |
| `platforms/` | Runtime, CUDA, Qt, graphics policies and shader compilation |
| `project/` | Project lifecycle, workspace components, developer tools, formatting, analysis, documentation and generated metadata |
| `docs/` | Capability, CUDA and delivery guides |

`testing/Workloads.cmake` registers both test and benchmark targets because they
share target setup. Their execution scripts live in `benchmark/`. The root
application framework under `../../cmake/` consumes this toolkit; the toolkit does
not include the application framework.

Direct script paths now follow the layout, for example:

```sh
cmake -DSOURCE_DIR=. -DMATRIX_PROFILE=core -P lib/cmake/build/BuildMatrix.cmake
cmake -DBASELINE=before.json -DCANDIDATE=after.json -P lib/cmake/benchmark/CompareResults.cmake
```

Shared presets live in `build/LibraryPresets.json`. Consumers that previously
included individual flat modules must update those paths; there are no per-file
compatibility wrappers. Consumers of `Bootstrap.cmake` and `Boilerplate.cmake`
need no include changes.
