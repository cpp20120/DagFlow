if(CMAKE_GENERATOR STREQUAL "Ninja" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND NOT MSVC)
  add_test(NAME dagflow_cmake_configuration_tests
    COMMAND ${CMAKE_COMMAND} "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
      "-DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/cmake-configuration-tests"
      "-DCXX=${CMAKE_CXX_COMPILER}" "-DCHECK_TBB=${DAGFLOW_BUILD_BENCH}"
      "-DTBB_DIR=${TBB_DIR}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake_configuration_tests.cmake")
  set_tests_properties(dagflow_cmake_configuration_tests PROPERTIES TIMEOUT 60 LABELS build)
endif()

add_test(NAME dagflow_dependency_selection_tests COMMAND ${CMAKE_COMMAND}
  "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
  "-DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/dependency-selection-tests"
  -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/dependency_selection_tests.cmake")
set_tests_properties(dagflow_dependency_selection_tests PROPERTIES TIMEOUT 30 LABELS build)

if(TARGET dagflow_example)
  add_test(NAME dagflow_example_basic COMMAND dagflow_example)
  set_tests_properties(dagflow_example_basic PROPERTIES TIMEOUT 30)
  foreach(example IN ITEMS task_scope cancellation graph parallel_for batch)
    add_test(NAME dagflow_example_${example} COMMAND dagflow_example_${example})
    set_tests_properties(dagflow_example_${example} PROPERTIES TIMEOUT 30)
  endforeach()
endif()

set(_dagflow_stress_test_target "")
if(TARGET dagflow_stress_bench)
  set(_dagflow_stress_test_target dagflow_stress_bench)
endif()
if(_dagflow_stress_test_target)
  add_test(NAME dagflow_main_harness_tests
    COMMAND ${CMAKE_COMMAND} "-DBINARY=$<TARGET_FILE:${_dagflow_stress_test_target}>"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/main_harness_tests.cmake")
  set_tests_properties(dagflow_main_harness_tests PROPERTIES TIMEOUT 120 LABELS "benchmark;schema")
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    boilerplate_add_test(dagflow_perf_control_tests SOURCES tests/perf_control_tests.cpp
      ARGS "$<TARGET_FILE:${_dagflow_stress_test_target}>" TIMEOUT 40 LABELS benchmark)
    add_dependencies(dagflow_perf_control_tests ${_dagflow_stress_test_target})
  endif()
endif()

if(TARGET dagflow_runtime_suite)
  boilerplate_add_harness(dagflow_campaign_check TARGET dagflow_runtime_suite
    CASES "${CMAKE_CURRENT_SOURCE_DIR}/bench/runtime_cases.json"
    ROUNDS 2 WARMUP_RUNS 0 TIMEOUT 10 METRICS run_p50_us payload_tasks_per_second
    INVARIANTS checksum METADATA migration_test=native)
  add_test(NAME dagflow_campaign_tests COMMAND ${CMAKE_COMMAND}
    "-DCONFIG=${CMAKE_CURRENT_BINARY_DIR}/harness/$<CONFIG>/dagflow_campaign_check.cmake"
    "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/campaign_tests.cmake")
  set_tests_properties(dagflow_campaign_tests PROPERTIES TIMEOUT 45 LABELS "build;harness")
  add_test(NAME dagflow_benchmark_suite_tests
    COMMAND ${CMAKE_COMMAND} "-DBINARY=$<TARGET_FILE:dagflow_runtime_suite>"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_suite_tests.cmake")
  set_tests_properties(dagflow_benchmark_suite_tests PROPERTIES TIMEOUT 120 LABELS "benchmark;schema")

endif()

function(dagflow_add_runtime_unit_test name source)
  boilerplate_add_test(${name} SOURCES ${source} LIBRARIES Threads::Threads TIMEOUT 60)
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

# These tests replace global new/delete for counting or failure injection.
# TSan provides strong definitions of those operators and cannot interpose them.
# Keep the tests in ordinary/ASan jobs; queue and runtime tests still run in TSan.
set(_dagflow_test_allocation_interposition TRUE)
if(MSVC OR BOILERPLATE_SANITIZER STREQUAL "thread")
  set(_dagflow_test_allocation_interposition FALSE)
endif()
if(BOILERPLATE_SANITIZER STREQUAL "thread")
  message(STATUS "TSan: allocation-interposition tests are covered by non-TSan configurations")
endif()
if(_dagflow_test_allocation_interposition)
  dagflow_add_runtime_unit_test(dagflow_queue_allocation_tests tests/queue_allocation_tests.cpp)
endif()

dagflow_add_runtime_unit_test(dagflow_topology_allocation_tests tests/topology_allocation_tests.cpp)
target_sources(dagflow_topology_allocation_tests PRIVATE src/scheduler.cpp src/parking_lot.cpp)

dagflow_add_runtime_unit_test(dagflow_accounting_allocation_tests tests/accounting_allocation_tests.cpp)
target_sources(dagflow_accounting_allocation_tests PRIVATE src/idle_accounting.cpp)

if(DAGFLOW_TARGET)
  # Bounded lifetime/race regressions; independent of disabled fuzz hooks.
  add_custom_target(dagflow_adversarial_tests)
  foreach(_case IN ITEMS
      pool_address_reuse_tls
      completion_last_credit_race
      parking_epoch_reuse
      reentrant_capture_cleanup
      multi_source_completion_failure_race
      completion_reentrant_registration
      graph_scope_unwind_cleanup
      cancelled_capture_reentry
      nested_helping_context_restore
      graph_cancel_continuation_reuse_race
      exception_payload_last_release)
    dagflow_add_runtime_unit_test(dagflow_${_case}_test tests/${_case}_test.cpp)
    target_link_libraries(dagflow_${_case}_test PRIVATE ${DAGFLOW_TARGET})
    set_tests_properties(dagflow_${_case}_test PROPERTIES LABELS "runtime;adversarial")
    add_dependencies(dagflow_adversarial_tests dagflow_${_case}_test)
  endforeach()

  # Replace only the allocation boundary, not global new/delete. This supports
  # exact failure barriers and live-allocation accounting under TSan as well.
  set(_dagflow_fault_sources ${DAGFLOW_RUNTIME_SOURCES})
  list(REMOVE_ITEM _dagflow_fault_sources src/runtime_memory.cpp)
  add_library(dagflow_adversarial_runtime STATIC EXCLUDE_FROM_ALL
    ${_dagflow_fault_sources} tests/adversarial_allocation.cpp)
  target_include_directories(dagflow_adversarial_runtime PUBLIC include)
  target_compile_features(dagflow_adversarial_runtime PUBLIC cxx_std_23)
  target_compile_definitions(dagflow_adversarial_runtime PRIVATE DAGFLOW_STATIC)
  target_link_libraries(dagflow_adversarial_runtime PUBLIC Threads::Threads)
  boilerplate_apply_optimization(dagflow_adversarial_runtime)
  if(DAGFLOW_RUNTIME_DIAGNOSTICS)
    target_compile_definitions(dagflow_adversarial_runtime PUBLIC DAGFLOW_RUNTIME_DIAGNOSTICS=1)
  endif()
  foreach(_case IN ITEMS combine_partial_registration_oom_race range_publication_rollback_lifetime)
    dagflow_add_runtime_unit_test(dagflow_${_case}_test tests/${_case}_test.cpp)
    target_link_libraries(dagflow_${_case}_test PRIVATE dagflow_adversarial_runtime)
    set_tests_properties(dagflow_${_case}_test PROPERTIES LABELS "runtime;adversarial")
    add_dependencies(dagflow_adversarial_tests dagflow_${_case}_test)
  endforeach()

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

  if(_dagflow_test_allocation_interposition AND DAGFLOW_ALLOCATOR STREQUAL "system")
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
    add_dependencies(boilerplate_tests ${_target})
  endif()
endforeach()
