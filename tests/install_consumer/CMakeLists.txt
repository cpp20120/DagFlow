cmake_minimum_required(VERSION 3.26)
project(DagFlowInstallConsumer LANGUAGES CXX)

find_package(DagFlow CONFIG REQUIRED)

add_executable(dagflow_install_consumer main.cpp)
target_link_libraries(dagflow_install_consumer PRIVATE DagFlow::DagFlow)
