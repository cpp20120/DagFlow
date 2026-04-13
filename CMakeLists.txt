cmake_minimum_required(VERSION 3.26)
project(
        DagFlow
        VERSION 1.0.0
        DESCRIPTION
        "High-performance C++20 thread pool with work-stealing and DAG support"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(THREAD_POOL_VERSION ${PROJECT_VERSION})

option(TP_BUILD_SHARED "Build DagFlow as shared library" ON)
option(TP_BUILD_STATIC "Build DagFlow as static library" OFF)
option(TP_BUILD_EXAMPLES "Build examples" ON)
option(TP_BUILD_BENCH "Build benchmarks" OFF)
option(TP_BUILD_TESTS "Build tests" OFF)
option(TP_INSTALL "Generate install target" ON)

# Создаем отдельные объектные библиотеки
add_library(
        dagflow_sources_static OBJECT
        include/dagflow.hpp
        include/chase_lev_deque.hpp
        include/qsbr_domain.hpp
        include/small_function.hpp
        include/small_vec.hpp
        include/task_graph.hpp
        include/thread_pool.hpp
        src/thread_pool.cpp)

add_library(
        dagflow_sources_shared OBJECT
        include/dagflow.hpp
        include/chase_lev_deque.hpp
        include/qsbr_domain.hpp
        include/small_function.hpp
        include/small_vec.hpp
        include/task_graph.hpp
        include/thread_pool.hpp
        src/thread_pool.cpp)

# Настройки для static версии
target_include_directories(
        dagflow_sources_static
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_compile_features(dagflow_sources_static PUBLIC cxx_std_23)
target_compile_definitions(dagflow_sources_static PRIVATE DAGFLOW_STATIC)

if(MSVC)
  target_compile_options(dagflow_sources_static PRIVATE /W4 /WX)
else()
  target_compile_options(dagflow_sources_static PRIVATE -Wall -Wextra -pedantic)
endif()

# Настройки для shared версии
target_include_directories(
        dagflow_sources_shared
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_compile_features(dagflow_sources_shared PUBLIC cxx_std_23)
set_property(TARGET dagflow_sources_shared PROPERTY POSITION_INDEPENDENT_CODE ON)

if(MSVC)
  target_compile_options(dagflow_sources_shared PRIVATE /W4 /WX)
else()
  target_compile_options(dagflow_sources_shared PRIVATE -Wall -Wextra -pedantic)
endif()

set(DAGFLOW_TARGET "")

if(TP_BUILD_SHARED)
  add_library(DagFlow SHARED
          include/config.hpp)
  target_sources(DagFlow PRIVATE $<TARGET_OBJECTS:dagflow_sources_shared>)

  set_target_properties(
          DagFlow
          PROPERTIES VERSION ${PROJECT_VERSION}
          SOVERSION 1
          POSITION_INDEPENDENT_CODE ON
          WINDOWS_EXPORT_ALL_SYMBOLS ON
          OUTPUT_NAME "dagflow")

  target_include_directories(
          DagFlow PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
          $<INSTALL_INTERFACE:include>)
  target_compile_features(DagFlow PUBLIC cxx_std_23)
  set(DAGFLOW_TARGET DagFlow)
endif()

if(TP_BUILD_STATIC)
  add_library(DagFlow_static STATIC)
  target_sources(DagFlow_static PRIVATE $<TARGET_OBJECTS:dagflow_sources_static>)

  set_target_properties(DagFlow_static PROPERTIES OUTPUT_NAME "dagflow")
  target_include_directories(
          DagFlow_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
          $<INSTALL_INTERFACE:include>)
  target_compile_features(DagFlow_static PUBLIC cxx_std_23)

  if(NOT DAGFLOW_TARGET)
    set(DAGFLOW_TARGET DagFlow_static)
  endif()
endif()

if(DAGFLOW_TARGET)
  add_library(DagFlow::DagFlow ALIAS ${DAGFLOW_TARGET})
endif()

if(TP_BUILD_EXAMPLES AND DAGFLOW_TARGET)
  add_executable(DagFlow_example src/main.cpp)
  target_link_libraries(DagFlow_example PRIVATE ${DAGFLOW_TARGET})
  set_target_properties(DagFlow_example PROPERTIES OUTPUT_NAME "dagflow-example")
endif()

if(TP_INSTALL)
  include(GNUInstallDirs)
  install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

  if(TP_BUILD_SHARED)
    install(
            TARGETS DagFlow
            EXPORT DagFlowTargets
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif()

  if(TP_BUILD_STATIC)
    install(
            TARGETS DagFlow_static
            EXPORT DagFlowTargets
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()

  install(
          EXPORT DagFlowTargets
          FILE DagFlowTargets.cmake
          NAMESPACE DagFlow::
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DagFlow)

  include(CMakePackageConfigHelpers)
  configure_package_config_file(
          cmake/DagFlowConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/DagFlowConfig.cmake
          INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DagFlow)

  write_basic_package_version_file(
          ${CMAKE_CURRENT_BINARY_DIR}/DagFlowConfigVersion.cmake
          VERSION ${PROJECT_VERSION}
          COMPATIBILITY SameMajorVersion)

  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/DagFlowConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/DagFlowConfigVersion.cmake
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/DagFlow)
endif()