cmake_minimum_required(VERSION 3.26)
include(cmake/DagFlowDependencies.cmake)
include(cmake/boilerplate/Bootstrap.cmake)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  boilerplate_bootstrap()
endif()
project(DagFlow VERSION 1.0.0
  DESCRIPTION "Work-stealing thread pool with DAG support" LANGUAGES CXX)

option(DAGFLOW_BUILD_EXAMPLES "Build examples" ${PROJECT_IS_TOP_LEVEL})
option(DAGFLOW_BUILD_RUNTIME_BENCH "Build the standalone runtime benchmark" OFF)
option(DAGFLOW_BUILD_RUNTIME_SUITE "Build the runtime scenario suite" ${DAGFLOW_BUILD_RUNTIME_BENCH})
option(DAGFLOW_BUILD_PUBLIC_API_BENCH "Build the DagFlow public API benchmark" OFF)
option(DAGFLOW_BUILD_GITHUB_BENCH "Build the runtime suite against the local GitHub snapshot" OFF)
option(DAGFLOW_BUILD_STRESS_BENCH "Build the producer/queue stress harness" OFF)
option(DAGFLOW_BUILD_FUNCTION_BENCH "Build the callable storage/dispatch benchmark" OFF)
option(DAGFLOW_RUNTIME_DIAGNOSTICS "Instrument runtime paths (not a timing baseline)" OFF)
option(DAGFLOW_INSTALL "Generate install targets" ${PROJECT_IS_TOP_LEVEL})
option(DAGFLOW_TEST_TINY_QUEUES "Use two-slot queues and batch=1 in a dedicated test build" OFF)
if(DAGFLOW_TEST_TINY_QUEUES)
  if(NOT DAGFLOW_BUILD_TESTS OR DAGFLOW_INSTALL)
    message(FATAL_ERROR "DAGFLOW_TEST_TINY_QUEUES requires DAGFLOW_BUILD_TESTS=ON and DAGFLOW_INSTALL=OFF")
  endif()
  # Queue capacities affect template layouts: use the same setting in every TU.
  add_compile_definitions(DAGFLOW_TEST_TINY_QUEUES=1)
endif()


option(DAGFLOW_BUILD_DOCS "Generate API documentation with Doxygen" OFF)
# The public reference currently permits incomplete comments and links to
# optional benchmark artifacts. The reusable module keeps strict defaults.
option(BOILERPLATE_DOC_WARN_IF_DOC_ERROR "Report invalid documentation and unresolved references" OFF)
option(BOILERPLATE_DOC_WARN_IF_UNDOCUMENTED "Warn about undocumented API symbols" OFF)
option(BOILERPLATE_DOC_WARN_NO_PARAMDOC "Warn about undocumented parameters and return values" OFF)
set(BOILERPLATE_DOC_MARKDOWN_ID_STYLE GITHUB CACHE STRING "Markdown heading IDs: DOXYGEN or GITHUB")
set(BOILERPLATE_DOC_INPUTS
  "${PROJECT_SOURCE_DIR}/include;${PROJECT_SOURCE_DIR}/docs/api.md;"
  "${PROJECT_SOURCE_DIR}/docs/how_it_works.md;${PROJECT_SOURCE_DIR}/docs/code_organization.md;"
  "${PROJECT_SOURCE_DIR}/docs/concepts.md;${PROJECT_SOURCE_DIR}/docs/pool-lifecycle.md;"
  "${PROJECT_SOURCE_DIR}/docs/scheduler-topology.md;${PROJECT_SOURCE_DIR}/docs/idle-accounting.md;"
  "${PROJECT_SOURCE_DIR}/docs/explicit-ownership.md;${PROJECT_SOURCE_DIR}/docs/task-scope-lifetime.md;"
  "${PROJECT_SOURCE_DIR}/docs/batch-submit.md;${PROJECT_SOURCE_DIR}/docs/cmake-capabilities-validation.md"
  CACHE STRING "Public API headers and user documentation")
set(BOILERPLATE_DOC_EXCLUDE_SYMBOLS "dagflow::detail;dagflow::detail::*"
  CACHE STRING "Internal symbols excluded from the documented API")
set(BOILERPLATE_DOC_MAINPAGE "${PROJECT_SOURCE_DIR}/docs/api.md" CACHE FILEPATH
  "DagFlow documentation main page")
option(DAGFLOW_ENABLE_PACKAGING "Generate CPack packages" OFF)
option(DAGFLOW_BUILD_HARNESS "Enable framework benchmark campaigns" OFF)
set(DAGFLOW_PROJECT_CAPABILITIES "developer" CACHE STRING "Standalone project capabilities")
# Keep the default checkout independent of a globally configured compiler
# cache. Users can explicitly choose ccache or sccache through the framework.
set(BOILERPLATE_COMPILER_CACHE "none" CACHE STRING "Compiler cache: auto, ccache, sccache, or none")
if(PROJECT_IS_TOP_LEVEL AND BOILERPLATE_COMPILER_CACHE STREQUAL "none")
  set(CMAKE_CXX_COMPILER_LAUNCHER "" CACHE STRING "C++ compiler launcher" FORCE)
endif()
if(PROJECT_IS_TOP_LEVEL)
  include(CTest)
endif()
set(BOILERPLATE_INSTALL "${DAGFLOW_INSTALL}")
include(cmake/boilerplate/Boilerplate.cmake)
get_property(_dagflow_parent_project GLOBAL PROPERTY BOILERPLATE_PROJECT_CONFIGURED)
if(NOT _dagflow_parent_project)
  set(_dagflow_caps ${DAGFLOW_PROJECT_CAPABILITIES})
  if(DAGFLOW_BUILD_FUZZERS)
    list(APPEND _dagflow_caps testing fuzzing)
  endif()
  if(DAGFLOW_BUILD_DOCS)
    list(APPEND _dagflow_caps docs)
  endif()
  if(DAGFLOW_ENABLE_PACKAGING)
    list(APPEND _dagflow_caps packaging)
  endif()
  boilerplate_project(TOP_LEVEL_CAPABILITIES ${_dagflow_caps}
    EMBEDDED_CAPABILITIES project-minimal)
endif()
find_package(Threads REQUIRED)
include(cmake/DagFlowRuntime.cmake)
include(cmake/DagFlowExamples.cmake)
include(cmake/DagFlowBenchmarks.cmake)
if(BUILD_TESTING)
  dagflow_add_benchmark_smoke_tests()
endif()
include(cmake/DagFlowWorkloads.cmake)
if(DAGFLOW_BUILD_TESTS AND BUILD_TESTING)
  enable_testing()
  include(cmake/DagFlowTests.cmake)
endif()
if(DAGFLOW_BUILD_FUZZERS)
  include(cmake/DagFlowFuzz.cmake)
endif()
if(DAGFLOW_BUILD_HARNESS)
  include(cmake/DagFlowHarness.cmake)
endif()
if(NOT _dagflow_parent_project)
  boilerplate_finalize_project()
endif()
