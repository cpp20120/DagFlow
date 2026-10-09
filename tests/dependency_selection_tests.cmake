cmake_minimum_required(VERSION 3.26)
file(MAKE_DIRECTORY "${CHECK_DIR}")
file(WRITE "${CHECK_DIR}/probe.cmake" "
cmake_minimum_required(VERSION 3.26)
set(CMAKE_SOURCE_DIR \"${SOURCE_DIR}\")
set(CMAKE_CURRENT_SOURCE_DIR \"${SOURCE_DIR}\")
if(EMBEDDED)
  set(CMAKE_SOURCE_DIR \"${CHECK_DIR}\")
endif()
include(\"${SOURCE_DIR}/cmake/DagFlowDependencies.cmake\")
if(NOT DAGFLOW_DEPENDENCY_PROVIDER STREQUAL EXPECT_PROVIDER OR
    NOT \"\${VCPKG_MANIFEST_FEATURES}\" STREQUAL EXPECT_FEATURES)
  message(FATAL_ERROR \"provider=\${DAGFLOW_DEPENDENCY_PROVIDER}; features=\${VCPKG_MANIFEST_FEATURES}\")
endif()
")
function(check name provider features)
  execute_process(COMMAND "${CMAKE_COMMAND}"
    "-DEXPECT_PROVIDER=${provider}" "-DEXPECT_FEATURES=${features}"
    ${ARGN} -P "${CHECK_DIR}/probe.cmake"
    RESULT_VARIABLE _result OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
  if(NOT _result EQUAL 0)
    message(FATAL_ERROR "${name}: ${_out}${_err}")
  endif()
endfunction()
check(core system "")
check(bench vcpkg tbb-bench -DDAGFLOW_BUILD_BENCH=ON)
check(mimalloc vcpkg mimalloc -DDAGFLOW_ALLOCATOR=mimalloc)
check(tbbmalloc vcpkg tbbmalloc -DDAGFLOW_ALLOCATOR=tbbmalloc)
check(combined vcpkg "tbb-bench;mimalloc" -DDAGFLOW_BUILD_BENCH=ON -DDAGFLOW_ALLOCATOR=mimalloc)
check(unused_allocator system "" -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_ALLOCATOR=mimalloc)
check(tbb_only vcpkg tbb-bench -DDAGFLOW_BUILD_SHARED=OFF -DDAGFLOW_ALLOCATOR=mimalloc -DDAGFLOW_BUILD_BENCH=ON)
check(system_override system "" -DDAGFLOW_BUILD_BENCH=ON -DDAGFLOW_DEPENDENCY_PROVIDER=system)
check(explicit_features vcpkg "mimalloc;tbb-bench" -DDAGFLOW_BUILD_BENCH=ON
  -DVCPKG_MANIFEST_FEATURES=mimalloc -DDAGFLOW_DEPENDENCY_PROVIDER=vcpkg)
check(embedded system parent -DEMBEDDED=ON -DDAGFLOW_BUILD_BENCH=ON
  -DDAGFLOW_DEPENDENCY_PROVIDER=system -DVCPKG_MANIFEST_FEATURES=parent)
message(STATUS "Dependency selection checks passed")
