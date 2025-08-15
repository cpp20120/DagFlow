cmake_minimum_required(VERSION 3.26)
project(ThreadPool
  VERSION 1.0.0
  DESCRIPTION "High-performance C++20 thread pool with work-stealing and DAG support"
  LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(THREAD_POOL_VERSION ${PROJECT_VERSION})

option(TP_BUILD_SHARED "Build threadpool as shared library" ON)
option(TP_BUILD_STATIC "Build threadpool as static library" OFF)
option(TP_BUILD_EXAMPLES "Build examples" ON)
option(TP_BUILD_BENCH "Build benchmarks" ON)
option(TP_BUILD_TESTS "Build tests" OFF)
option(TP_INSTALL "Generate install target" ON)

add_library(threadpool_sources
  include/api.hpp
  include/chase_lev_deque.hpp
  include/hazard_ptr.hpp
  include/mpmc_queue.hpp
  include/small_function.hpp
  include/small_vec.hpp
  include/task.hpp
  include/task_graph.hpp
  include/thread_pool.hpp
  src/thread_pool.cpp
)

if(TP_BUILD_SHARED)
  add_library(ThreadPool SHARED)
  target_sources(ThreadPool PRIVATE $<TARGET_OBJECTS:threadpool_sources>)
  
  set_target_properties(ThreadPool PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    POSITION_INDEPENDENT_CODE ON
    WINDOWS_EXPORT_ALL_SYMBOLS ON
    OUTPUT_NAME "threadpool"
  )
endif()

if(TP_BUILD_STATIC)
  add_library(ThreadPoolStatic STATIC)
  target_sources(ThreadPoolStatic PRIVATE $<TARGET_OBJECTS:threadpool_sources>)
  set_target_properties(ThreadPoolStatic PROPERTIES OUTPUT_NAME "threadpool")
endif()

foreach(target IN ITEMS ThreadPool ThreadPoolStatic)
  if(TARGET ${target})
    target_include_directories(${target} PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>
    )
    target_compile_features(${target} PUBLIC cxx_std_23)
    
    if(MSVC)
      target_compile_options(${target} PRIVATE /W4 /WX)
    else()
      target_compile_options(${target} PRIVATE -Wall -Wextra -pedantic)
    endif()
  endif()
endforeach()

if(TP_BUILD_EXAMPLES)
  add_executable(threadpool_example src/main.cpp)
  target_link_libraries(threadpool_example PRIVATE ThreadPool)
  set_target_properties(threadpool_example PROPERTIES OUTPUT_NAME "threadpool-example")
endif()

if(TP_BUILD_BENCH)
  find_package(benchmark QUIET)
    add_executable(threadpool_bench bench/tbb_bench.cpp)
    target_link_libraries(threadpool_bench PRIVATE ThreadPool benchmark::benchmark)
    set_target_properties(threadpool_bench PROPERTIES OUTPUT_NAME "threadpool-bench")
endif()


if(TP_INSTALL)
  include(GNUInstallDirs)
  
  install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  
  if(TP_BUILD_SHARED)
    install(TARGETS ThreadPool
      EXPORT ThreadPoolTargets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
  endif()
  
  if(TP_BUILD_STATIC)
    install(TARGETS ThreadPoolStatic
      EXPORT ThreadPoolTargets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
  endif()
  
  install(EXPORT ThreadPoolTargets
    FILE ThreadPoolConfig.cmake
    NAMESPACE ThreadPool::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ThreadPool
  )
  
  include(CMakePackageConfigHelpers)
  configure_package_config_file(
    cmake/ThreadPoolConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/ThreadPoolConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ThreadPool
  )
  
  write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/ThreadPoolConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
  )
  
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/ThreadPoolConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/ThreadPoolConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ThreadPool
  )
endif()