# Configuration regressions: no workload measurements or profile generation.
file(MAKE_DIRECTORY "${CHECK_DIR}")

function(configure_case name expected)
  execute_process(COMMAND "${CMAKE_COMMAND}" -S "${SOURCE_DIR}"
    -B "${CHECK_DIR}/${name}" -G Ninja "-DCMAKE_CXX_COMPILER=${CXX}"
    -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_BUILD_STATIC=OFF -DDAGFLOW_BUILD_TESTS=OFF
    -DDAGFLOW_BUILD_EXAMPLES=OFF -DDAGFLOW_INSTALL=OFF -DDAGFLOW_BUILD_BENCH=OFF
    -DDAGFLOW_BUILD_RUNTIME_BENCH=OFF -DDAGFLOW_BUILD_RUNTIME_SUITE=OFF
    -DDAGFLOW_COMPILER_CACHE=none -DDAGFLOW_PROJECT_CAPABILITIES=project-minimal
    -DDAGFLOW_ALLOCATOR=mimalloc -DCMAKE_DISABLE_FIND_PACKAGE_mimalloc=ON
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
configure_case(invalid_icf "DAGFLOW_ENABLE_ICF requires DAGFLOW_USE_LLD=ON"
  -DDAGFLOW_ENABLE_ICF=ON -DDAGFLOW_USE_LLD=OFF)

# PGO applies to sources appended after dagflow_apply_target_policy as well.
set(probe "${CHECK_DIR}/late-source")
file(MAKE_DIRECTORY "${probe}")
file(WRITE "${probe}/training.profdata" "configure-only profile placeholder")
file(WRITE "${probe}/main.cpp" "int extra(); int main() { return extra(); }\n")
file(WRITE "${probe}/late.cpp" "int extra() { return 0; }\n")
file(WRITE "${probe}/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.26)
project(ProfileDependency LANGUAGES CXX)
set(DAGFLOW_PGO_MODE use CACHE STRING \"\")
set(DAGFLOW_PGO_PROFILE \"${probe}/training.profdata\" CACHE FILEPATH \"\")
include(\"${SOURCE_DIR}/cmake/DagFlow.cmake\")
add_executable(probe main.cpp)
dagflow_apply_target_policy(probe)
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
