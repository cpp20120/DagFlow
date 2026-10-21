cmake_minimum_required(VERSION 3.26)
project(BenchmarkRunArguments LANGUAGES CXX)
include("${DAGFLOW_SOURCE_DIR}/cmake/dagflow/DagFlow.cmake")
include("${DAGFLOW_SOURCE_DIR}/cmake/DagFlowBenchmarks.cmake")

dagflow_add_executable(argv_probe SOURCES main.cpp)
dagflow_add_benchmark_run(argv_probe positional "4 4096 9")
dagflow_add_benchmark_run(argv_probe quoted "--label \"two words\" --workers 2")
dagflow_add_benchmark_run(argv_probe cached "invalid default")
dagflow_add_benchmark_run(argv_probe empty "")

# The generic scenario API must keep accepting CMake lists, including an
# individual argument containing whitespace.
dagflow_add_scenario(direct TARGET argv_probe GROUP dagflow
  ARGS --label "two words" --workers 2 REPEATS 1 WARMUP 0)

if(NOT "$CACHE{DAGFLOW_CACHED_ARGS}" STREQUAL "--label \"two words\" --workers 2")
  message(FATAL_ERROR "The command-line cache override was rewritten")
endif()
