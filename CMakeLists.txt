cmake_minimum_required(VERSION 3.26)
include(cmake/Bootstrap.cmake)
dagflow_bootstrap()
project(DagFlow VERSION 1.0.0
  DESCRIPTION "Work-stealing thread pool with DAG support" LANGUAGES CXX)

option(DAGFLOW_BUILD_SHARED "Build DagFlow as a shared library" ON)
option(DAGFLOW_BUILD_STATIC "Build DagFlow as a static library" OFF)
option(DAGFLOW_BUILD_EXAMPLES "Build examples" ${PROJECT_IS_TOP_LEVEL})
option(DAGFLOW_BUILD_BENCH "Build the oneTBB comparison benchmark" OFF)
option(DAGFLOW_BUILD_RUNTIME_BENCH "Build the standalone runtime benchmark" OFF)
option(DAGFLOW_BUILD_RUNTIME_SUITE "Build the runtime scenario suite" ${DAGFLOW_BUILD_RUNTIME_BENCH})
option(DAGFLOW_BUILD_PUBLIC_API_BENCH "Build the DagFlow public API benchmark" OFF)
option(DAGFLOW_BUILD_GITHUB_BENCH "Build the runtime suite against the local GitHub snapshot" OFF)
option(DAGFLOW_BUILD_STRESS_BENCH "Build the producer/queue stress harness" OFF)
option(DAGFLOW_BUILD_FUNCTION_BENCH "Build the callable storage/dispatch benchmark" OFF)
option(DAGFLOW_BUILD_TESTS "Build tests" OFF)
option(DAGFLOW_RUNTIME_DIAGNOSTICS "Instrument runtime paths (not a timing baseline)" OFF)
option(DAGFLOW_INSTALL "Generate install targets" ${PROJECT_IS_TOP_LEVEL})


option(DAGFLOW_BUILD_DOCS "Generate API documentation with Doxygen" OFF)
# The public reference currently permits incomplete comments and links to
# optional benchmark artifacts. The reusable module keeps strict defaults.
option(DAGFLOW_DOC_WARN_IF_DOC_ERROR "Report invalid documentation and unresolved references" OFF)
option(DAGFLOW_DOC_WARN_IF_UNDOCUMENTED "Warn about undocumented API symbols" OFF)
option(DAGFLOW_DOC_WARN_NO_PARAMDOC "Warn about undocumented parameters and return values" OFF)
set(DAGFLOW_DOC_MARKDOWN_ID_STYLE GITHUB CACHE STRING "Markdown heading IDs: DOXYGEN or GITHUB")
set(DAGFLOW_DOC_INPUTS
  "${PROJECT_SOURCE_DIR}/include;${PROJECT_SOURCE_DIR}/docs/api.md;"
  "${PROJECT_SOURCE_DIR}/docs/how_it_works.md;${PROJECT_SOURCE_DIR}/docs/code_organization.md;"
  "${PROJECT_SOURCE_DIR}/docs/concepts.md;${PROJECT_SOURCE_DIR}/docs/pool-lifecycle.md;"
  "${PROJECT_SOURCE_DIR}/docs/scheduler-topology.md;${PROJECT_SOURCE_DIR}/docs/idle-accounting.md;"
  "${PROJECT_SOURCE_DIR}/docs/explicit-ownership.md;${PROJECT_SOURCE_DIR}/docs/task-scope-lifetime.md;"
  "${PROJECT_SOURCE_DIR}/docs/batch-submit.md;${PROJECT_SOURCE_DIR}/docs/cmake-capabilities-validation.md"
  CACHE STRING "Public API headers and user documentation")
set(DAGFLOW_DOC_EXCLUDE_SYMBOLS "dagflow::detail;dagflow::detail::*"
  CACHE STRING "Internal symbols excluded from the documented API")
set(DAGFLOW_DOC_MAINPAGE "${PROJECT_SOURCE_DIR}/docs/api.md" CACHE FILEPATH
  "DagFlow documentation main page")
option(DAGFLOW_ENABLE_PACKAGING "Generate CPack packages" OFF)
option(DAGFLOW_BUILD_HARNESS "Enable framework benchmark campaigns (requires Python)" OFF)
set(DAGFLOW_ALLOCATOR "mimalloc" CACHE STRING "Runtime allocator: mimalloc, tbbmalloc, or system")
set(DAGFLOW_PROJECT_CAPABILITIES "developer" CACHE STRING "Standalone project capabilities")
# Keep the default checkout independent of a globally configured compiler
# cache. Users can explicitly choose ccache or sccache through the framework.
set(DAGFLOW_COMPILER_CACHE "none" CACHE STRING "Compiler cache: auto, ccache, sccache, or none")
if(DAGFLOW_COMPILER_CACHE STREQUAL "none")
  set(CMAKE_CXX_COMPILER_LAUNCHER "" CACHE STRING "C++ compiler launcher" FORCE)
endif()
if(PROJECT_IS_TOP_LEVEL)
  include(CTest)
endif()
include(cmake/DagFlow.cmake)
get_property(_dagflow_parent_project GLOBAL PROPERTY DAGFLOW_PROJECT_CONFIGURED)
if(NOT _dagflow_parent_project)
  set(_dagflow_caps ${DAGFLOW_PROJECT_CAPABILITIES})
  if(DAGFLOW_BUILD_DOCS)
    list(APPEND _dagflow_caps docs)
  endif()
  if(DAGFLOW_ENABLE_PACKAGING)
    list(APPEND _dagflow_caps packaging)
  endif()
  dagflow_project(TOP_LEVEL_CAPABILITIES ${_dagflow_caps}
    EMBEDDED_CAPABILITIES project-minimal)
endif()
find_package(Threads REQUIRED)
include(cmake/DagFlowRuntime.cmake)
include(cmake/DagFlowExamples.cmake)
include(cmake/DagFlowBenchmarks.cmake)
include(cmake/DagFlowWorkloads.cmake)
if(DAGFLOW_BUILD_TESTS AND BUILD_TESTING)
  enable_testing()
  include(cmake/DagFlowTests.cmake)
endif()
if(DAGFLOW_BUILD_HARNESS)
  include(cmake/DagFlowHarness.cmake)
endif()
if(NOT _dagflow_parent_project)
  dagflow_finalize_project()
endif()
