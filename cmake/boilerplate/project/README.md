# Project capabilities

Project capabilities own repository/application lifecycle. They do **not** replace
target policies.

```text
Bootstrap.cmake       pre-project toolchain/provider + in-source guard
ProjectCapabilities  configure/finalize composition
TargetPolicies       compile/link semantics of one real CMake target
Workloads/Harness    execution and experiment workflows
```

Typical root:

```cmake
cmake_minimum_required(VERSION 3.26)
include(cmake/Bootstrap.cmake)
boilerplate_bootstrap()
project(app VERSION 1.0 LANGUAGES CXX)
include(CTest)
include(cmake/Boilerplate.cmake)

boilerplate_project(
  TOP_LEVEL_CAPABILITIES developer
  EMBEDDED_CAPABILITIES project-minimal)

boilerplate_add_executable(app SOURCES src/main.cpp
  POLICIES runtime-webserver)

boilerplate_finalize_project()
```

## Primitives

| Capability | Responsibility |
| --- | --- |
| `project-minimal` | no project-owned developer/distribution workflow |
| `compile-commands` | exports `compile_commands.json` |
| `compiler-cache` | configures `sccache`/`ccache` launcher when available |
| `formatting` | `format`, `format-check`, CMake formatting aggregate targets |
| `static-analysis` | clang-tidy/cppcheck/IWYU/clazy aggregate targets |
| `testing` | CTest/check lifecycle (top-level CMake still calls `include(CTest)`) |
| `property-testing` | aggregate RapidCheck/property-test lifecycle; inherits `testing` |
| `fuzzing` | libFuzzer/AFL++/honggfuzz byte harnesses and Google FuzzTest lifecycle; inherits `testing` |
| `coverage-report` | lcov/genhtml workflow; instrumentation is target policy `coverage` |
| `docs` | Doxygen target over configured source roots |
| `packaging` | component-aware CPack configuration |
| `reproducible-build` | enables reproducible source/debug path mapping by default |
| `build-info` | JSON manifest + `boilerplate::build_info` generated C++ header |
| `diagnostics` | resolved project/target summary + `boilerplate_config` target |
| `cuda` | explicitly enables/configures the CUDA language |
| `web-deployment` | aggregate deployment for registered Emscripten targets |

Compositions: `developer`, `quality`, `distribution`, `ci`, `full`.

Capabilities are named compositions, not mutually-exclusive project profiles. Define
project-local ones with `boilerplate_define_project_capability(... INHERITS ...
CONFIGURE_HOOKS ... FINALIZE_HOOKS ...)`.


## Components and large workspaces

`boilerplate_project()` remains intentionally single-instance for one CMake configure.
Large source trees scale below it with components rather than by creating nested project
lifecycles:

```text
workspace lifecycle
├── component A
│   ├── target
│   └── target
├── component B
│   └── target
└── unscoped framework/workspace targets
```

Use `boilerplate_add_component(name SOURCE_DIR ... [BINARY_DIR ...] [FOLDER ...]
[EXCLUDE_FROM_ALL] [SYSTEM])` as an `add_subdirectory()` replacement when the root wants
to name and group a subtree. `boilerplate_component(name ...)` registers the current
directory instead and is useful when the same subtree can be configured standalone.
Framework artifact constructors automatically call `boilerplate_register_target()`; raw
CMake targets only need explicit registration when they should belong to a component.
Project-wide workflows still inspect unregistered raw targets. Finalization freezes the
semantic target graph and caches the complete recursive target walk, so multiple finalize
hooks do not repeatedly traverse a large directory tree.

The registry is intentionally metadata, not a second dependency graph. Link/dependency
edges remain ordinary CMake target edges, target policies remain target-local, and project
capabilities remain workspace-wide. This avoids a `ProjectContext` cross product at scale.

## Dependency provider

[Capability fixes and checks](../docs/CapabilityChecks.md) documents the tool,
coverage, CUDA, dependency and Doxygen controls and their regression checks.

`Bootstrap.cmake` exposes `BOILERPLATE_DEPENDENCY_PROVIDER`:
`none`, `system`, `vcpkg`, `fetchcontent`, `cpm`. vcpkg toolchain selection happens
before `project()`. `boilerplate_require_dependency()` first accepts an existing/imported
package target; fetch providers require an explicit pinned repository/tag. Domain policies
therefore do not know which package manager supplied Qt/Vulkan/Google packages.

## Migration from the old root modules

- `CompilerSettings.cmake` / `LinkerSettings.cmake` / `BuildConfiguration.cmake` -> target policies.
- `CodeFormatAndAnalysis.cmake` -> `formatting`, `static-analysis`, `compiler-cache`.
- `CodeCoverage.cmake` -> target `coverage` + project `coverage-report`.
- `CPackConfig.cmake` -> project `packaging`.
- `ShaderCompilation.cmake` -> `boilerplate_add_glsl_shaders()`.
- global vcpkg setup -> `Bootstrap.cmake`/toolchain selection before `project()`.
