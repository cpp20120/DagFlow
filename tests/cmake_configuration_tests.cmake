cmake_minimum_required(VERSION 3.26)
# Configuration regressions: no workload measurements or profile generation.
file(MAKE_DIRECTORY "${CHECK_DIR}")

function(configure_case name expected)
  execute_process(COMMAND "${CMAKE_COMMAND}" -S "${SOURCE_DIR}"
    -B "${CHECK_DIR}/${name}" -G Ninja "-DCMAKE_CXX_COMPILER=${CXX}"
    -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_BUILD_STATIC=OFF -DDAGFLOW_BUILD_TESTS=OFF
    -DDAGFLOW_BUILD_EXAMPLES=OFF -DDAGFLOW_INSTALL=OFF -DDAGFLOW_BUILD_BENCH=OFF
    -DDAGFLOW_BUILD_RUNTIME_BENCH=OFF -DDAGFLOW_BUILD_RUNTIME_SUITE=OFF
    -DBOILERPLATE_COMPILER_CACHE=none -DDAGFLOW_PROJECT_CAPABILITIES=project-minimal
    -DDAGFLOW_ALLOCATOR=mimalloc -DCMAKE_DISABLE_FIND_PACKAGE_mimalloc=ON
    -DBOILERPLATE_DEPENDENCY_PROVIDER=system "-DTBB_DIR=${TBB_DIR}"
    ${ARGN} RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
  file(WRITE "${CHECK_DIR}/${name}.log" "${output}\n${error}")
  if(expected STREQUAL "success")
    if(NOT result EQUAL 0)
      message(FATAL_ERROR "${name}: ${output}\n${error}")
    endif()
  elseif(result EQUAL 0 OR NOT "${output}\n${error}" MATCHES "${expected}")
    message(FATAL_ERROR "${name}: expected diagnostic '${expected}': ${output}\n${error}")
  endif()
endfunction()

# An unused allocator must not become a configure dependency, including TBB-only.
configure_case(no_runtime success)
if(CHECK_TBB)
  configure_case(tbb_only success -DDAGFLOW_BUILD_BENCH=ON)
endif()
configure_case(invalid_icf "BOILERPLATE_ENABLE_ICF requires BOILERPLATE_USE_LLD=ON"
  -DBOILERPLATE_ENABLE_ICF=ON -DBOILERPLATE_USE_LLD=OFF)

# PGO applies to sources appended after boilerplate_apply_target_policy as well.
set(probe "${CHECK_DIR}/late-source")
file(MAKE_DIRECTORY "${probe}")
file(WRITE "${probe}/training.profdata" "configure-only profile placeholder")
file(WRITE "${probe}/main.cpp" "int extra(); int main() { return extra(); }\n")
file(WRITE "${probe}/late.cpp" "int extra() { return 0; }\n")
file(WRITE "${probe}/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.26)
project(ProfileDependency LANGUAGES CXX)
set(BOILERPLATE_PGO_MODE use CACHE STRING \"\")
set(BOILERPLATE_PGO_PROFILE \"${probe}/training.profdata\" CACHE FILEPATH \"\")
include(\"${SOURCE_DIR}/cmake/boilerplate/Boilerplate.cmake\")
add_executable(probe main.cpp)
boilerplate_apply_target_policy(probe)
target_sources(probe PRIVATE late.cpp)
")
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${probe}" -B "${probe}/build"
  -G Ninja "-DCMAKE_CXX_COMPILER=${CXX}"
  RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "Late-source configuration failed: ${output}\n${error}")
endif()
file(READ "${probe}/build/build.ninja" ninja)
foreach(source IN ITEMS main late)
  string(REGEX MATCH "build [^\n]*${source}\\.cpp\\.(o|obj): [^\n]+" rule "${ninja}")
  if(NOT rule MATCHES "training\\.profdata")
    message(FATAL_ERROR "${source}.cpp lacks the profile input dependency: ${rule}")
  endif()
endforeach()
message(STATUS "CMake dependency and configuration regressions passed")

# Every project capability must resolve its own executable. A stale cache from
# older builds must not turn clang-tidy/coverage/etc. into the first tool found.
include("${SOURCE_DIR}/cmake/boilerplate/project/ProjectCapabilities.cmake")
set(_tool "${CMAKE_COMMAND}" CACHE FILEPATH "Legacy shared tool lookup" FORCE)
boilerplate_project_tool(_cmake "CMake regression probe" NAMES cmake)
boilerplate_project_tool(_ctest "CTest regression probe" NAMES ctest)
boilerplate_project_tool(_missing "Optional regression probe" NAMES dagflow-tool-that-does-not-exist)
if(NOT _cmake OR NOT _ctest OR _cmake STREQUAL _ctest OR _missing)
  message(FATAL_ERROR "Project tool lookups are not independent: ${_cmake};${_ctest};${_missing}")
endif()
message(STATUS "Independent project tool lookup regression passed")
boilerplate_list_project_capabilities(_capabilities)
foreach(_cap IN ITEMS project-minimal compile-commands compiler-cache formatting static-analysis
    testing property-testing fuzzing coverage-report docs packaging reproducible-build
    build-info diagnostics cuda web-deployment developer quality distribution ci full)
  if(NOT _cap IN_LIST _capabilities)
    message(FATAL_ERROR "Missing project capability: ${_cap}")
  endif()
endforeach()
get_property(_coverage_caps GLOBAL PROPERTY BOILERPLATE_PROJECT_CAP_COVERAGE_REPORT_CLOSURE)
if(NOT "testing" IN_LIST _coverage_caps)
  message(FATAL_ERROR "coverage-report must inherit testing to build tests before collecting data")
endif()

# Disabled standalone sources must not be passed to analysis tools without a
# compile command (notably optional fuzz/property fixtures and benchmarks).
set(analysis_probe "${CHECK_DIR}/analysis-sources")
file(MAKE_DIRECTORY "${analysis_probe}/src")
file(WRITE "${analysis_probe}/src/main.cpp" "int main() { return 0; }\n")
file(WRITE "${analysis_probe}/src/disabled.cpp" "#error not enabled in this build\n")
file(WRITE "${analysis_probe}/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.26)
project(AnalysisSources LANGUAGES CXX)
include(\"${SOURCE_DIR}/cmake/boilerplate/Boilerplate.cmake\")
boilerplate_project(CAPABILITIES static-analysis)
add_executable(probe src/main.cpp)
boilerplate_finalize_project()
")
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${analysis_probe}" -B "${analysis_probe}/build"
  -G Ninja "-DCMAKE_CXX_COMPILER=${CXX}"
  RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "Analysis source configuration failed: ${output}\n${error}")
endif()
file(READ "${analysis_probe}/build/build.ninja" analysis_ninja)
if(analysis_ninja MATCHES "COMMAND = [^\n]*disabled\\.cpp")
  message(FATAL_ERROR "Static analysis includes a disabled source")
endif()
