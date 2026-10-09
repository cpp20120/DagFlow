cmake_minimum_required(VERSION 3.26)
project(DagFlowInstallConsumer LANGUAGES CXX)

if(DAGFLOW_SOURCE_DIR)
  add_subdirectory("${DAGFLOW_SOURCE_DIR}" dagflow)
else()
  find_package(DagFlow CONFIG REQUIRED)
endif()

add_executable(dagflow_install_consumer main.cpp)
set(DAGFLOW_CONSUMER_VARIANT "preferred" CACHE STRING "Package variant: preferred, shared or static")
set_property(CACHE DAGFLOW_CONSUMER_VARIANT PROPERTY STRINGS preferred shared static)
if(DAGFLOW_CONSUMER_VARIANT STREQUAL "preferred")
  set(_dagflow_consumer_target DagFlow::DagFlow)
elseif(DAGFLOW_CONSUMER_VARIANT MATCHES "^(shared|static)$")
  set(_dagflow_consumer_target DagFlow::DagFlow_${DAGFLOW_CONSUMER_VARIANT})
else()
  message(FATAL_ERROR "Unknown DAGFLOW_CONSUMER_VARIANT=${DAGFLOW_CONSUMER_VARIANT}")
endif()
if(NOT TARGET ${_dagflow_consumer_target})
  message(FATAL_ERROR "Package does not export ${_dagflow_consumer_target}")
endif()
target_link_libraries(dagflow_install_consumer PRIVATE ${_dagflow_consumer_target})
if(WIN32)
  add_custom_command(TARGET dagflow_install_consumer POST_BUILD
    COMMAND ${CMAKE_COMMAND} "-DRUNTIME_FILES=$<TARGET_RUNTIME_DLLS:dagflow_install_consumer>"
      "-DDESTINATION=$<TARGET_FILE_DIR:dagflow_install_consumer>"
      -P "${CMAKE_CURRENT_LIST_DIR}/../../cmake/dagflow/packaging/CopyRuntimeLibraries.cmake"
    VERBATIM)
endif()
enable_testing()
add_test(NAME installed_consumer COMMAND dagflow_install_consumer)
