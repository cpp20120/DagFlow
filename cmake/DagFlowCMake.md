# Reusable CMake API

Recipes, knobs and preset commands are in the [root README](../README.md).
Copy `cmake/` into a project, call `project()` and `include(CTest)`, then include
`cmake/DagFlow.cmake` in the common parent directory. CMake 3.26+ is required.
The root application and standalone `lib/` entry points use the same modules.

## Targets

```cmake
dagflow_add_library(my_math
  VERSION 1.2.0
  SOURCES src/math.cpp
  INCLUDE_DIR include
  PUBLIC_LIBRARIES Threads::Threads
  PRIVATE_LIBRARIES some_dependency
  PACKAGE_CONFIG cmake/MyMathConfig.cmake.in)

dagflow_add_executable(my_app
  SOURCES src/main.cpp LIBRARIES my_math::my_math INSTALL)

dagflow_add_test(my_test
  SOURCES tests/math.cpp LIBRARIES my_math::my_math
  ARGS --small LABELS math TIMEOUT 10
  ENVIRONMENT "MODE=test")

dagflow_add_example(math_example
  SOURCES examples/math.cpp
  LIBRARIES my_math::my_math
  ARGS --demo
  SMOKE SMOKE_ARGS --quick
  PGO PGO_ARGS --training-size 100)
```

`dagflow_add_example()` is sugar over the same executable/scenario/check/PGO
primitives used elsewhere. Every example joins the `dagflow_examples` build
aggregate and the `examples` scenario group, so it gets `run_<target>` and the
shared `run_group_examples` runner. `SMOKE` adds a bounded CTest check; `PGO`
registers an optional training workload. Neither execution path is part of the
normal build. `INSTALL` is available when an example is also a shipped tool.

`VERSION`, library dependencies and `PACKAGE_CONFIG` are optional. The checked-in
`library1` and `library2` examples deliberately exercise different consumer-local
policy modules; `library2` additionally demonstrates a public `Threads::Threads`
dependency, a matching `find_dependency(Threads)` package config and PCH. The explicit
`dagflow_check_cmake` target installs/relocates both examples to keep those paths
covered.

Export macros
are derived from the library name: `my_math_export.h`, `MY_MATH_EXPORT`,
`MY_MATH_STATIC_DEFINE`. Include the export header in public headers and annotate
exported functions/classes. Static/shared are real CMake library types. Aliases:
`my_math::shared`, `my_math::static`, `my_math::my_math` and build-tree `my_math`.
Per-library `<NAME>_BUILD_SHARED/STATIC` override global defaults; at least one is
required for each declared library. The preferred alias selects shared if available.

Installation puts each library's headers under `include/<name>`, exports native
artifacts and package configs, and propagates static instrumentation link requirements.
Start custom package configs from `LibraryConfig.cmake.in` and add dependencies with
`find_dependency()`. The examples use identical `include.hpp` names only in isolation;
real projects should give public headers distinct names. Installed PGO/ThinLTO
artifacts may require matching compiler/profile inputs; plain release packages are
more portable. Application install RPATH depends on the final layout: the root
example sets it to the sibling library directory; the generic helper does not guess.

For existing owned targets call `dagflow_apply_target_policy(target)` in their
source directory. `dagflow_apply_optimization(target)` is kept as a compatible
legacy name. Both apply the common project options once. Third-party imported/
FetchContent targets are not modified. `dagflow_set_output_name` adds the
effective target artifact suffix. Dependencies still use ordinary CMake targets;
there is no separate dependency graph or custom package manager.

## Project capabilities and lifecycle

For GLSL/HLSL variants, MODULE plugins, a versioned ABI/reload example, and
relocatable application installation with runtime dependencies and resources,
see [Shaders, plugins and application delivery](cmake/Delivery.md).

The same reusable `cmake/` tree now covers whole-project concerns without turning them
into target flags. Include `Bootstrap.cmake` before `project()` when using its in-source
guard/dependency-provider bootstrap, then configure capabilities after `DagFlow.cmake`:

```cmake
cmake_minimum_required(VERSION 3.26)
include(cmake/Bootstrap.cmake)
dagflow_bootstrap()
project(server VERSION 1.0 LANGUAGES CXX)
include(CTest)
include(cmake/DagFlow.cmake)

dagflow_project(
  TOP_LEVEL_CAPABILITIES developer
  EMBEDDED_CAPABILITIES project-minimal)

dagflow_add_executable(server SOURCES src/main.cpp
  POLICIES runtime-webserver)
dagflow_finalize_project()
```

The composition unit is intentionally different at each level:

- bootstrap: toolchain/dependency provider and in-source guard, before `project()`;
- project capabilities: docs, analysis, tests, packaging, metadata and lifecycle workflows;
- target policies: warnings, hardening, sanitizer/LTO/PGO, runtime, Qt and graphics semantics;
- workloads: CTest/scenarios/PGO/Python harness.

Primitive project capabilities are `project-minimal`, `compile-commands`,
`compiler-cache`, `formatting`, `static-analysis`, `testing`, `property-testing`, `fuzzing`,
`coverage-report`, `docs`, `packaging`, `reproducible-build`, `build-info`,
`diagnostics`, `cuda`, and `web-deployment`. `developer`, `quality`, `distribution`,
`ci` and `full` are convenience compositions. Capabilities compose through configure and
finalize hooks, so a project-local module can define another capability with
`dagflow_define_project_capability()` without adding a preset cross product.

`PROJECT_IS_TOP_LEVEL` selects the top-level or embedded capability list. This is the
important difference from a single developer-mode switch: an embedded library can remain
`project-minimal` while the standalone repository enables format/analyze/tests/docs.
Optional developer tools degrade to omitted custom targets unless
`DAGFLOW_REQUIRE_PROJECT_TOOLS=ON`.

`build-info` creates an installed JSON manifest and the build-tree interface target
`dagflow::build_info`; link it when C++ code needs version/revision/compiler metadata.
`distribution` enables reproducible source/debug path mapping as the project default and
component-aware CPack packaging (`Runtime`, `Development`, `Documentation`). A target can
still opt back out with `POLICIES minimal`. Coverage keeps instrumentation (`coverage`
target policy) separate from report generation (`coverage-report` project capability).

## Target policies and composition

`DAGFLOW_PROFILE` remains a **project/configuration default** used by presets
(build type and default LTO/PGO choices). It is not the composition axis. The
composition unit is a CMake target. Named target policies can reset or override the
project defaults independently for each library/executable:

```cmake
# A domain policy for a scheduler/runtime. Policies listed in INHERITS compose
# left-to-right, so later entries win.
dagflow_define_policy(runtime
  INHERITS minimal native thin-lto lld gc-sections no-plt)

dagflow_add_library(my_runtime
  SOURCES src/runtime.cpp
  INCLUDE_DIR include
  POLICIES runtime)

# Same project, deliberately boring std-only target.
dagflow_add_executable(tool
  SOURCES tools/tool.cpp
  POLICIES minimal)

# One-off escape hatch without inventing another named profile.
dagflow_add_test(runtime_test
  SOURCES tests/runtime.cpp
  LIBRARIES my_runtime::my_runtime
  POLICIES minimal tsan
  POLICY_OPTIONS WARNINGS_AS_ERRORS OFF)
```

Predefined orthogonal policies are `minimal` (`cpp-minimal` alias), `hardened` (`hardening` alias), `coverage`,
`debug-symbols`, `frame-pointers`, `werror`, `ccache`, `clang-tidy`, `unity`, `native`,
`thin-lto`, `full-lto`, `lld`, `gc-sections`, `no-plt`, `no-semantic-interposition`,
`icf`, `cfi`, `cfi-icall`, `cfi-vcall`, `windows-cfg`, `asan`, `ubsan`,
`asan-ubsan`, `tsan`, `lsan`, `pgo-generate`, and `pgo-use`.
`minimal` explicitly removes
expensive optimization/instrumentation/tooling defaults (LTO, PGO, sanitizer, CFI, CFG,
native tuning, clang-tidy, ccache, etc.) while keeping the project's selected C++
standard and artifact naming. This is the small denominator for `src/ + include/`
projects.

Policies are just reusable settings; they do not replace direct target-level CMake.
For an existing target the equivalent is:

```cmake
add_library(core STATIC core.cpp)
dagflow_apply_target_policy(core POLICIES minimal native full-lto)

# Or configure first, then apply later.
dagflow_configure_target(core POLICIES minimal
  LTO_MODE full ENABLE_NATIVE ON)
dagflow_apply_target_policy(core)
```

Settings compose as `project defaults -> POLICIES (left-to-right) -> explicit
POLICY_OPTIONS`. `dagflow_add_library` also accepts `SHARED_POLICIES`,
`STATIC_POLICIES`, `SHARED_POLICY_OPTIONS` and `STATIC_POLICY_OPTIONS`; these are
appended after the common library policy for the corresponding real target. Use
`dagflow_get_target_setting(target KEY out)` when another module needs the
resolved value, and `dagflow_print_target_policy(target)` for configure-time
diagnostics.

Policies also support `HOOKS` for domain mechanics that are not compiler flags.
That is the intended extension point for Qt, Vulkan/GLFW/ImGui, shader pipelines,
etc., without teaching the generic library/runtime substrate about them:

```cmake
function(project_qt_widgets target)
  find_package(Qt6 REQUIRED COMPONENTS Core Gui Widgets)
  set_target_properties(${target} PROPERTIES AUTOMOC ON AUTOUIC ON AUTORCC ON)
  target_link_libraries(${target} PRIVATE Qt6::Core Qt6::Gui Qt6::Widgets)
endfunction()

dagflow_define_policy(qt-widgets
  HOOKS project_qt_widgets)

dagflow_add_executable(editor
  SOURCES src/editor.cpp
  POLICIES minimal qt-widgets)
```

A hook receives the target name and runs once after the generic target policy was
applied. Domain-specific modules can therefore define `runtime`, `qt-widgets`,
`vulkan-imgui`, or project-local policies as compositions instead of multiplying
presets. Build configuration (`Debug`/`Release`, compiler/toolchain, generator) stays
orthogonal and belongs in presets/toolchain files.


### Control-flow protection

Control-flow protection is target policy, independent of the existing `SANITIZER`
setting. The implementation lives in `ControlFlow.cmake` and is loaded by the
normal `DagFlow.cmake` entry point; no project-specific flag scripts are needed.

| Policy | Effective settings / behavior |
| --- | --- |
| `cfi` | Clang's `-fsanitize=cfi` schemes, ThinLTO, lld, hidden default visibility |
| `cfi-icall` | Type checks on indirect function calls, ThinLTO, lld |
| `cfi-vcall` | Type checks on virtual calls, ThinLTO, lld, hidden default visibility |
| `windows-cfg` | `/guard:cf` compilation and `/GUARD:CF /DYNAMICBASE` linking |
| `runtime-hardened` | `runtime` + `hardening` + `cfi`; also links Threads |

```cmake
dagflow_add_executable(game
  SOURCES src/main.cpp
  POLICIES runtime-hardened)

# A narrower check, Full LTO instead of the policy's ThinLTO default,
# and diagnostic output before termination.
dagflow_add_executable(tool
  SOURCES tools/main.cpp
  POLICIES minimal cfi-icall
  POLICY_OPTIONS LTO_MODE full CFI_DIAGNOSTICS ON)

# Windows is a separate backend; it does not check C++ function signatures.
dagflow_add_executable(windows_game
  SOURCES src/main.cpp
  POLICIES runtime hardening windows-cfg)
```

Select the example matching the toolchain: this implementation supports CFI on
Linux with GNU-style Clang, and CFG on Windows with MSVC/clang-cl. Other platforms,
CFI without LTO and mixed CFI/CFG fail at configure time. A compile-and-link probe
checks the actual linker and sanitizer-runtime combination. CFG also rejects
the `/ZI` Edit and Continue and `/clr` modes in the project compiler flags.

The project defaults are `DAGFLOW_CFI=none|cfi|cfi-icall|cfi-vcall`,
`DAGFLOW_CFI_DIAGNOSTICS=OFF` and `DAGFLOW_WINDOWS_CFG=OFF`. The matching
target keys are `CFI`, `CFI_DIAGNOSTICS` and `WINDOWS_CFG`. Setting the CFI cache
variable alone requires an explicit `DAGFLOW_LTO_MODE=thin|full` and an
LTO-capable linker (normally `DAGFLOW_USE_LLD=ON`). Named CFI policies already
supply these defaults; later policies or `POLICY_OPTIONS` can override them.
CFI traps by default. Diagnostics mode reports the violation and terminates;
it does not recover and continue execution. `minimal` resets all three settings.

Apply CFI to every owned target that contains code you want instrumented, including
static libraries. Static/object targets forward final-link requirements; they do
not instrument their consumers' source files. Shared-library checks cover that
library's own link unit. Diagnostic shared libraries also forward the UBSan runtime
link requirement to their executable consumers. This does **not** enable cross-DSO
CFI: DLL/plugin callbacks, exported polymorphic classes and scripting/JIT boundaries
need a deliberate ABI design. Explicitly exported classes may have public LTO
visibility and therefore no virtual-call CFI checks. See the
[Clang CFI documentation](https://clang.llvm.org/docs/ControlFlowIntegrity.html).
Windows CFG has different guarantees; see
[Microsoft's CFG documentation](https://learn.microsoft.com/en-us/cpp/build/reference/guard-enable-control-flow-guard).

`dagflow_check_control_flow` is available with Ninja plus Clang/lld on Linux,
or Ninja plus MSVC/clang-cl on Windows. It builds real fixtures, runs valid calls,
checks CFI rejection of mismatched indirect/virtual calls in both trap and
diagnostic modes, and checks Windows EXE/DLL CFG metadata using `dumpbin`.
Linux CI runs CFI checks; Windows CI runs CFG checks.

### Built-in domain policies

The reusable layer now ships three domain modules. Runtime policies are archetypes;
Qt/graphics policies are pure capabilities and therefore do **not** reset whatever
optimization policy was already selected on the target.

```cmake
# Portable runtime baseline; links Threads::Threads.
dagflow_add_library(core
  SOURCES src/core.cpp INCLUDE_DIR include
  POLICIES runtime)

# CPU-bound scheduler/executor: native + full LTO + lld + section GC + ICF,
# no PLT and no ELF semantic interposition.
dagflow_add_library(scheduler
  SOURCES src/scheduler.cpp INCLUDE_DIR include
  POLICIES runtime-dagflow)

# Event-loop/server runtime: native + ThinLTO + lld + section GC + ICF + no PLT;
# semantic interposition remains enabled because DSOs/plugins are common.
dagflow_add_executable(server
  SOURCES src/server.cpp
  POLICIES runtime-webserver)

# Diagnostics deliberately start from the portable runtime baseline.
dagflow_add_test(race_test SOURCES tests/race.cpp
  LIBRARIES scheduler::scheduler POLICIES runtime-tsan)
```

Runtime policies: `runtime`, `runtime-hardened`, `runtime-dagflow`, `runtime-webserver`,
`runtime-profiled`, `runtime-asan`, `runtime-tsan`,
`runtime-dagflow-profiled`, `runtime-webserver-profiled`. The two tuned runtime
profiles intentionally target Clang/lld-style local performance work; `runtime-webserver`
uses ThinLTO. Use `runtime` plus orthogonal primitives for GCC/MSVC or portable distribution artifacts.

Qt policies: `qt-core`, `qt-gui`, `qt-widgets`, `qt-network`, `qt-qml`,
`qt-quick`, `qt-concurrent`, `qt-sql`, `qt-test`, plus `qt-desktop` and
`qt-qml-app` convenience compositions. They call `find_package(Qt6)`
only when applied. For example:

```cmake
dagflow_add_executable(editor
  SOURCES src/editor.cpp
  POLICIES minimal qt-widgets qt-network)
```

Graphics policies: `vulkan`, `glfw`, `glew`, `glm`, `imgui`,
`graphics-vulkan`, `graphics-opengl`, `graphics-vulkan-imgui`,
`graphics-opengl-imgui`, and kitchen-sink `graphics` (Vulkan + GLFW + GLEW + GLM + ImGui). `imgui` accepts an existing target through
`DAGFLOW_IMGUI_TARGET`; otherwise common imported target names are detected.
Shader sources need parameters and are therefore an explicit helper rather than a
policy:

```cmake
dagflow_add_executable(viewer SOURCES src/viewer.cpp
  POLICIES minimal graphics-vulkan-imgui)
dagflow_add_glsl_shaders(viewer
  SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/shaders"
  COPY_TO_RUNTIME)
```

The useful composition rule is: **archetype first, capabilities after it**. Thus
`POLICIES runtime-dagflow qt-network` keeps the runtime tuning, while a Qt policy
never smuggles `minimal` in and resets earlier settings.

Source-aware mechanics stay explicit rather than becoming fake booleans. Use
`dagflow_enable_pch(target HEADERS <...> ...)` for PCH,
`dagflow_add_glsl_shaders()` for shader inputs, and
`dagflow_enable_qt_deployment(target)` when an installed Qt application should
use Qt's native generated deployment script. `unity` remains an ordinary target
policy because it needs no project-specific input.

## General Python process harness

`Scenarios.cmake` remains a zero-Python lightweight runner suitable for smoke tests,
PGO registration and simple named workloads. More serious benchmark/stress campaigns
use the optional Python harness under `cmake/harness/`: CMake owns target construction and
passes resolved build-policy metadata; Python owns process lifecycle, CPU affinity,
randomized rounds, command/provenance logs, JSON parsing, statistics and optional
Linux `perf stat` counters.

```cmake
dagflow_add_harness(runtime_matrix
  TARGET runtime_bench
  CASES "${CMAKE_CURRENT_SOURCE_DIR}/bench/cases.json"
  ROUNDS 9 WARMUP_RUNS 2 TIMEOUT 120
  AFFINITY physical RANDOMIZE
  METRICS run_p50_us payload_tasks_per_second
  INVARIANTS checksum)
```

The cases JSON contains project semantics (`scenario`, workers, task counts encoded
as executable arguments); the harness itself knows nothing about DAGs, HTTP, shards,
frames or allocators. Running `run_runtime_matrix` creates a self-contained result
directory with `manifest.json`, `commands.json`, `runs.jsonl`, `summary.json`, and
stdout/stderr logs. The Python package also exposes `CommandRunner`, `digest`,
`physical_cpus`, `parse_perf_stat`, and `save_json` so project-specific research
scripts can progressively drop their duplicated plumbing without being forced into
the declarative case runner. See `cmake/harness/README.md`.

## Benchmarks and named scenarios

```cmake
dagflow_add_benchmark(my_bench GROUP custom
  SOURCES bench/custom.cpp LIBRARIES my_math::my_math
  ARGS --tasks 10000)

# An individual/group filter may have disabled the executable.
if(TARGET my_bench)
  dagflow_add_scenario(small TARGET my_bench GROUP custom
    ARGS --tasks 10000
    SMOKE SMOKE_ARGS --tasks 1
    PGO PGO_ARGS --tasks 100
    ENVIRONMENT "DATA_PATH=${CMAKE_CURRENT_SOURCE_DIR}/data"
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    REPEATS 7 WARMUP 2 TIMEOUT 30
    LABELS api)
endif()

dagflow_add_google_benchmark(my_google_bench GROUP google
  SOURCES bench/google.cpp LIBRARIES my_math::my_math
  ARGS --benchmark_filter=MyOperation)
```

Each benchmark gets `run_<target>`, each extra scenario gets `run_<name>`.
Names/groups accept letters, digits, `_`, `-`. Arguments are CMake lists, not shell
strings. Shell expansion is never used. `COMMAND_PREFIX` can supply an explicit
list such as `perf;stat;--`. Tests run the executable directly via CTest; measurements
use `RunWorkload.cmake`. `SMOKE` and `PGO` explicitly opt into those registrations;
`SMOKE_ARGS` / `PGO_ARGS` do not inherit measurement arguments accidentally.
The PGO registration inherits the scenario environment, cwd and timeout.

Google Benchmark uses `benchmark::benchmark_main`; benchmark source files register
operations without supplying another main. Native default flags request five
repetitions and JSON output. Its outer process harness runs once with no extra
warmup. Google registration smoke only lists operations. To check execution manually
without a measurement campaign, use `--benchmark_min_time=1x --benchmark_repetitions=1`.
Per-target arguments are overridable via `DAGFLOW_<TARGET>_ARGS`.

`dagflow_register_check(name target ARGS ... LABELS ... TIMEOUT ...
ENVIRONMENT ... WORKING_DIRECTORY ...)` registers an existing executable with CTest.
`dagflow_tests` builds check binaries; `dagflow_check` builds them and runs
CTest. GoogleTest uses `dagflow_add_googletest(name SOURCES ... LIBRARIES ...
LABELS ...)`, optionally `OWN_MAIN`. It discovers cases through native CMake's
GoogleTest module. Call helpers only for enabled components; they do not download
or discover dependencies until needed.

## Training, external projects and upstream comparison

```cmake
dagflow_pgo_workload(my_test --small)
dagflow_add_reference(old_version
  SOURCE_DIR "${REFERENCE_SOURCE}"
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
  TEST INSTALL)

dagflow_add_google_comparison(compare_google
  SCRIPT "${GOOGLE_BENCHMARK_SOURCE}/tools/compare.py"
  BASELINE "${CMAKE_BINARY_DIR}/before.json"
  CANDIDATE "${CMAKE_BINARY_DIR}/after.json")
```

PGO targets exist in generate mode after the first workload is registered. Workloads
run in registration order, with failures/timeouts propagated. LLVM merge uses
`MergeProfiles.cmake`; profile filenames include target/process IDs. Relative training
paths use the caller's binary directory unless a scenario specifies a working directory.

`dagflow_add_reference` builds an explicit CMake source tree with ExternalProject.
It is excluded from ALL and has its own binary/install directories. `TEST` and
`INSTALL` are optional. Supply compatible compiler/profile arguments yourself.
For non-CMake sources use CMake's ExternalProject directly with explicit commands.
`dagflow_add_google_comparison` calls the upstream Python tool through CMake;
it does not replace Google's result schema or statistical comparison.

## Allocator and fuzz adapters

```cmake
dagflow_use_allocator(my_app ALLOCATOR mimalloc PREFIX MY_APP)
dagflow_add_fuzzer(parser_fuzz SOURCES fuzz/parser.cpp LIBRARIES parser)
```

Allocator choice defaults to `DAGFLOW_ALLOCATOR`; PREFIX defaults to
`DAGFLOW`. The application implements allocation/free branches using the generated
compile definitions. No allocator replacement is automatic. Fuzzers use Clang
libFuzzer + address/undefined sanitizers, a build-tree corpus, a bounded smoke and
an explicit `run_<name>`. Instrument linked code when seeking coverage there.

## File map

| Module | Responsibility |
| --- | --- |
| `BuildProfiles.cmake` | Named profiles and compiler/mode validation |
| `TargetPolicies.cmake` | Named target-policy composition, inheritance, overrides and domain hooks |
| `RuntimePolicies.cmake`, `QtPolicies.cmake`, `GraphicsPolicies.cmake` | Built-in runtime archetypes and optional Qt/graphics capabilities |
| `ProjectOptions.cmake`, `TargetOptions.cmake` | Target-scoped language, warnings, tools, hardening, coverage, optimizer/sanitizer/PGO flags |
| `Library.cmake`, `LibraryConfig.cmake.in` | Library variants, export headers, relocatable CMake packages |
| `Dependencies.cmake` | Optional Google packages, allocator adapters, ExternalProject, upstream comparison |
| `Workloads.cmake` | Custom/Google benchmarks, plain/Google tests, fuzzers, PGO |
| `Scenarios.cmake`, `RunWorkload.cmake` | Lightweight CMake-only named scenarios and PGO/smoke process runner |
| `Harness.cmake`, `cmake/harness/` | Optional Python matrix runner, provenance, affinity, statistics and perf counters |
| `Bootstrap.cmake`, `ProjectCapabilities.cmake`, `project/*.cmake` | Pre-project bootstrap plus project lifecycle/tooling capabilities |
| `BuildMatrix.cmake`, `BuildInfo.cmake` | Preset sequencing and compatibility include for build metadata |
| `CompareResults.cmake` | Matching process-result median comparison |
| `DagFlowLibraryPresets.json` | Shared standalone/root library presets and workflows |
