dagflow_add_benchmark_smoke_tests()
if(CMAKE_GENERATOR STREQUAL "Ninja" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND NOT MSVC)
  add_test(NAME dagflow_cmake_configuration_tests
    COMMAND ${CMAKE_COMMAND} "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
      "-DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/cmake-configuration-tests"
      "-DCXX=${CMAKE_CXX_COMPILER}" "-DCHECK_TBB=${DAGFLOW_BUILD_BENCH}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake_configuration_tests.cmake")
  set_tests_properties(dagflow_cmake_configuration_tests PROPERTIES TIMEOUT 60 LABELS build)
endif()

if(TARGET DagFlow_example)
  add_test(NAME dagflow_example_basic COMMAND DagFlow_example
    --workers 2 --tasks 128 --warmup 0 --warmup-ms 0 --repeats 1
    --bursts 2 --idle-us 100 --verify exact)
  set_tests_properties(dagflow_example_basic PROPERTIES TIMEOUT 30)
  find_package(Python3 QUIET COMPONENTS Interpreter)
  if(Python3_Interpreter_FOUND)
    add_test(NAME dagflow_main_harness_tests
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/main_harness_tests.py
              $<TARGET_FILE:DagFlow_example>)
    set_tests_properties(dagflow_main_harness_tests PROPERTIES TIMEOUT 120)
  endif()
  foreach(example IN ITEMS task_scope cancellation graph parallel_for batch)
    add_test(NAME dagflow_example_${example} COMMAND DagFlow_example_${example})
    set_tests_properties(dagflow_example_${example} PROPERTIES TIMEOUT 30)
  endforeach()
endif()

if(TARGET dagflow_runtime_suite)
  find_package(Python3 QUIET COMPONENTS Interpreter)
  if(Python3_Interpreter_FOUND)
    add_test(NAME dagflow_benchmark_suite_tests
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_suite_tests.py
              $<TARGET_FILE:dagflow_runtime_suite> ${CMAKE_CURRENT_SOURCE_DIR}/scripts/benchmark_suite.py)
    set_tests_properties(dagflow_benchmark_suite_tests PROPERTIES TIMEOUT 120)
  endif()
endif()

function(dagflow_add_runtime_unit_test name source)
  dagflow_add_test(${name} SOURCES ${source} LIBRARIES Threads::Threads TIMEOUT 60)
  target_include_directories(${name} PRIVATE include)
endfunction()

dagflow_add_runtime_unit_test(dagflow_queue_tests tests/queue_tests.cpp)
dagflow_add_runtime_unit_test(dagflow_container_tests tests/container_tests.cpp)
if(DAGFLOW_TARGET)
  target_link_libraries(dagflow_container_tests PRIVATE ${DAGFLOW_TARGET})
else()
  target_sources(dagflow_container_tests PRIVATE src/runtime_memory.cpp)
  dagflow_apply_allocator(dagflow_container_tests)
endif()

# A recording runtime_memory backend checks allocation requests directly.
dagflow_add_runtime_unit_test(dagflow_stl_allocation_tests tests/stl_allocation_tests.cpp)
dagflow_add_runtime_unit_test(dagflow_vector_allocation_tests tests/vector_allocation_tests.cpp)
dagflow_add_runtime_unit_test(dagflow_function_allocation_tests tests/function_tests.cpp)
target_compile_definitions(dagflow_function_allocation_tests PRIVATE DAGFLOW_FUNCTION_TEST_BACKEND)

if(NOT MSVC)
  dagflow_add_runtime_unit_test(dagflow_queue_allocation_tests tests/queue_allocation_tests.cpp)
endif()

dagflow_add_runtime_unit_test(dagflow_topology_allocation_tests tests/topology_allocation_tests.cpp)
target_sources(dagflow_topology_allocation_tests PRIVATE src/scheduler.cpp src/parking_lot.cpp)

dagflow_add_runtime_unit_test(dagflow_accounting_allocation_tests tests/accounting_allocation_tests.cpp)
target_sources(dagflow_accounting_allocation_tests PRIVATE src/idle_accounting.cpp)

if(DAGFLOW_TARGET)
  dagflow_add_runtime_unit_test(dagflow_idle_accounting_tests tests/idle_accounting_tests.cpp)
  target_link_libraries(dagflow_idle_accounting_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_scheduler_topology_tests tests/scheduler_topology_tests.cpp)
  target_link_libraries(dagflow_scheduler_topology_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_function_tests tests/function_tests.cpp)
  target_link_libraries(dagflow_function_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_pool_queue_tests tests/pool_queue_tests.cpp)
  target_link_libraries(dagflow_pool_queue_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_pool_lifecycle_tests tests/pool_lifecycle_tests.cpp)
  target_link_libraries(dagflow_pool_lifecycle_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_scaling_tests tests/scaling_tests.cpp)
  target_link_libraries(dagflow_scaling_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_batch_submit_tests tests/batch_submit_tests.cpp)
  target_link_libraries(dagflow_batch_submit_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_runtime_memory_tests tests/runtime_memory_tests.cpp)
  target_link_libraries(dagflow_runtime_memory_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_ownership_tests tests/ownership_tests.cpp)
  target_link_libraries(dagflow_ownership_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_api_convenience_tests tests/api_convenience_tests.cpp)
  target_link_libraries(dagflow_api_convenience_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_pool_memory_tests tests/pool_memory_tests.cpp)
  target_link_libraries(dagflow_pool_memory_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_task_scope_tests tests/task_scope_tests.cpp)
  target_link_libraries(dagflow_task_scope_tests PRIVATE ${DAGFLOW_TARGET})

  dagflow_add_runtime_unit_test(dagflow_graph_tests tests/graph_tests.cpp)
  target_link_libraries(dagflow_graph_tests PRIVATE ${DAGFLOW_TARGET})

  if(NOT MSVC AND DAGFLOW_ALLOCATOR STREQUAL "system")
    dagflow_add_runtime_unit_test(dagflow_runtime_failure_tests tests/runtime_failure_tests.cpp)
    target_link_libraries(dagflow_runtime_failure_tests PRIVATE ${DAGFLOW_TARGET})
  endif()

  dagflow_add_runtime_unit_test(dagflow_scheduler_fairness_tests tests/scheduler_fairness_tests.cpp)
  target_link_libraries(dagflow_scheduler_fairness_tests PRIVATE ${DAGFLOW_TARGET})
endif()

get_property(_dagflow_targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
foreach(_target IN LISTS _dagflow_targets)
  get_target_property(_type ${_target} TYPE)
  if(_type STREQUAL "EXECUTABLE")
    add_dependencies(dagflow_tests ${_target})
  endif()
endforeach()
