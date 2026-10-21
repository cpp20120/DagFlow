cmake_minimum_required(VERSION 3.26)
execute_process(COMMAND "${CMAKE_COMMAND}" --fresh
  -S "${SOURCE_DIR}/tests/benchmark_run" -B "${CHECK_DIR}"
  -G "${GENERATOR}" "-DCMAKE_CXX_COMPILER=${CXX}"
  "-DDAGFLOW_SOURCE_DIR=${SOURCE_DIR}" -DCMAKE_BUILD_TYPE=Debug
  "-DDAGFLOW_CACHED_ARGS=--label \"two words\" --workers 2"
  RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT "${result}" STREQUAL "0")
  message(FATAL_ERROR "Benchmark fixture configure failed: ${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" --build "${CHECK_DIR}"
  --config Debug --target run_group_dagflow
  RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error TIMEOUT 60)
if(NOT "${result}" STREQUAL "0")
  message(FATAL_ERROR "Benchmark run targets failed: ${output}\n${error}")
endif()
message(STATUS "Benchmark run argument regressions passed")
