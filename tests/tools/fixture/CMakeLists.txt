cmake_minimum_required(VERSION 3.26)
project(DagFlowBridgeFixture LANGUAGES CXX)
add_executable(dagflow_fuzz_queues worker.cpp)
include("${CMAKE_CURRENT_LIST_DIR}/../../../cmake/DagFlowTargetBridge.cmake")
if(DAGFLOW_TOOLS_EXECUTABLE)
  dagflow_add_target_bridge(dagflow_bridge_fuzz_queues
    TARGET dagflow_fuzz_queues ARGS "--fixture" "hello world")
endif()
