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

add_test(NAME dagflow_benchmark_run_tests COMMAND ${CMAKE_COMMAND}
  "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
  "-DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/benchmark-run-tests"
  "-DGENERATOR=${CMAKE_GENERATOR}" "-DCXX=${CMAKE_CXX_COMPILER}"
  -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_run_tests.cmake")
set_tests_properties(dagflow_benchmark_run_tests PROPERTIES TIMEOUT 90 LABELS "build;benchmark")

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
    dagflow_add_test(dagflow_perf_control_tests SOURCES tests/perf_control_tests.cpp
      ARGS "$<TARGET_FILE:${_dagflow_stress_test_target}>" TIMEOUT 40 LABELS benchmark)
    add_dependencies(dagflow_perf_control_tests ${_dagflow_stress_test_target})
  endif()
endif()

if(TARGET dagflow_runtime_suite)
  dagflow_add_harness(dagflow_campaign_check TARGET dagflow_runtime_suite
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
  dagflow_add_test(${name} SOURCES ${source} LIBRARIES Threads::Threads TIMEOUT 60)
  target_include_directories(${name} PRIVATE include)
endfunction()

dagflow_add_runtime_unit_test(dagflow_queue_tests tests/queue_tests.cpp)
set_property(TEST dagflow_queue_tests APPEND PROPERTY LABELS concurrency)
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
if(MSVC OR DAGFLOW_SANITIZER STREQUAL "thread")
  set(_dagflow_test_allocation_interposition FALSE)
endif()
if(DAGFLOW_SANITIZER STREQUAL "thread")
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
  # Keep the codec, deterministic generator, replay and valid-trace shrinker
  # covered independently of any individual runtime model.
  dagflow_add_runtime_unit_test(dagflow_property_support_tests
    tests/property_support_tests.cpp)
  set_tests_properties(dagflow_property_support_tests PROPERTIES
    LABELS "runtime;property;pbt")

  # Explicit state-machine properties with generated commands, valid-trace
  # shrinking and replay. Keep core dependency-free; these are ordinary tests,
  # independent of the optional libFuzzer/RapidCheck capability fixtures.
  add_custom_target(dagflow_model_property_tests)
  add_dependencies(dagflow_model_property_tests dagflow_property_support_tests)
  foreach(_component IN ITEMS storage queue completion pool scope graph range
      scheduler parking accounting packet topology wait coverage_boundary
      branch_protocol branch_closure public_api detail_vector_boundary
      detail_accounting_boundary graph_scope_lifecycle)
    set(_property_target dagflow_${_component}_property_tests)
    dagflow_add_runtime_unit_test(${_property_target}
      tests/${_component}_property_tests.cpp)
    target_link_libraries(${_property_target} PRIVATE ${DAGFLOW_TARGET})
    set_tests_properties(${_property_target} PROPERTIES
      LABELS "runtime;property;pbt;stateful")
    add_dependencies(dagflow_model_property_tests ${_property_target})
  endforeach()

  # Independent DAG specification and bounded real-time concurrent history
  # checker. Both use the existing seed/replay/shrink stateful runner.
  dagflow_add_runtime_unit_test(dagflow_reference_graph_model_property_tests
    tests/reference_graph_model_property_tests.cpp)
  target_link_libraries(dagflow_reference_graph_model_property_tests PRIVATE ${DAGFLOW_TARGET})
  set_tests_properties(dagflow_reference_graph_model_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;reference-model" TIMEOUT 120)
  add_dependencies(dagflow_model_property_tests dagflow_reference_graph_model_property_tests)

  dagflow_add_runtime_unit_test(dagflow_pool_reference_property_tests
    tests/pool_reference_property_tests.cpp)
  target_link_libraries(dagflow_pool_reference_property_tests PRIVATE ${DAGFLOW_TARGET})
  set_tests_properties(dagflow_pool_reference_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;reference-model" TIMEOUT 120)
  add_dependencies(dagflow_model_property_tests dagflow_pool_reference_property_tests)

  dagflow_add_runtime_unit_test(dagflow_concurrent_history_property_tests
    tests/concurrent_history_property_tests.cpp)
  target_link_libraries(dagflow_concurrent_history_property_tests PRIVATE ${DAGFLOW_TARGET})
  set_tests_properties(dagflow_concurrent_history_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;concurrency;history-check" TIMEOUT 120)
  add_dependencies(dagflow_model_property_tests dagflow_concurrent_history_property_tests)

  dagflow_add_runtime_unit_test(dagflow_completion_failure_reference_property_tests
    tests/completion_failure_reference_property_tests.cpp)
  target_link_libraries(dagflow_completion_failure_reference_property_tests PRIVATE ${DAGFLOW_TARGET})
  set_tests_properties(dagflow_completion_failure_reference_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;reference-model;completion" TIMEOUT 120)
  add_dependencies(dagflow_model_property_tests dagflow_completion_failure_reference_property_tests)

  # Explicit opt-in mutation campaign: compiles disposable copies of one
  # queue header and executes the bounded history oracle against each mutant.
  # Not part of ordinary CTest/coverage builds and never changes production.
  find_package(Python3 COMPONENTS Interpreter QUIET)
  if(Python3_Interpreter_FOUND)
    add_custom_target(dagflow_queue_mutation_campaign
      COMMAND ${Python3_EXECUTABLE}
        "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mutation_campaign.py"
        --cxx "${CMAKE_CXX_COMPILER}"
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
      COMMENT "Run isolated mutation campaign against the MPMC history checker"
      USES_TERMINAL)
  endif()

  if(Python3_Interpreter_FOUND AND UNIX AND
      CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_custom_target(dagflow_protocol_mutation_campaign
      COMMAND ${Python3_EXECUTABLE}
        "${CMAKE_CURRENT_SOURCE_DIR}/scripts/protocol_mutation_campaign.py"
        --cxx "${CMAKE_CXX_COMPILER}"
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
      COMMENT "Mutation-check TaskGraph and Completion against independent models"
      USES_TERMINAL)
  endif()

  # Compile a separate, instrumented runtime solely for deterministic
  # exceptional-path PBT. Never link hooks into the production DagFlow target.
  # Simulated counter saturation and wait/publication errors do not mutate
  # private state or require undefined behavior.
  add_library(dagflow_test_fault_runtime STATIC EXCLUDE_FROM_ALL
    ${DAGFLOW_RUNTIME_SOURCES})
  target_include_directories(dagflow_test_fault_runtime PUBLIC include)
  target_compile_features(dagflow_test_fault_runtime PUBLIC cxx_std_23)
  target_compile_definitions(dagflow_test_fault_runtime PRIVATE
    DAGFLOW_STATIC DAGFLOW_TEST_FAULT_INJECTION DAGFLOW_FUZZ_HOOKS)
  target_link_libraries(dagflow_test_fault_runtime PUBLIC Threads::Threads)
  dagflow_apply_allocator(dagflow_test_fault_runtime)
  dagflow_apply_optimization(dagflow_test_fault_runtime)
  if(DAGFLOW_RUNTIME_DIAGNOSTICS)
    target_compile_definitions(dagflow_test_fault_runtime PUBLIC
      DAGFLOW_RUNTIME_DIAGNOSTICS=1)
  endif()
  dagflow_add_runtime_unit_test(dagflow_fault_injection_property_tests
    tests/fault_injection_property_tests.cpp)
  target_compile_definitions(dagflow_fault_injection_property_tests PRIVATE
    DAGFLOW_TEST_FAULT_INJECTION)
  target_link_libraries(dagflow_fault_injection_property_tests PRIVATE
    dagflow_test_fault_runtime)
  set_tests_properties(dagflow_fault_injection_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;fault-injection")
  add_dependencies(dagflow_model_property_tests
    dagflow_fault_injection_property_tests)


  # Production code stays hook-free; this is an independent stateful fixture
  # for backpressure retries, allocation failure and boundary validation.
  dagflow_add_runtime_unit_test(dagflow_branch_gap_fault_property_tests
    tests/branch_gap_fault_property_tests.cpp)
  target_compile_definitions(dagflow_branch_gap_fault_property_tests PRIVATE
    DAGFLOW_TEST_FAULT_INJECTION DAGFLOW_FUZZ_HOOKS)
  target_link_libraries(dagflow_branch_gap_fault_property_tests PRIVATE
    dagflow_test_fault_runtime)
  set_tests_properties(dagflow_branch_gap_fault_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;fault-injection")
  add_dependencies(dagflow_model_property_tests dagflow_branch_gap_fault_property_tests)

  dagflow_add_runtime_unit_test(dagflow_graph_options_property_tests
    tests/graph_options_property_tests.cpp)
  target_compile_definitions(dagflow_graph_options_property_tests PRIVATE
    DAGFLOW_TEST_FAULT_INJECTION DAGFLOW_FUZZ_HOOKS)
  target_link_libraries(dagflow_graph_options_property_tests PRIVATE
    dagflow_test_fault_runtime)
  set_tests_properties(dagflow_graph_options_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful;fault-injection")
  add_dependencies(dagflow_model_property_tests
    dagflow_graph_options_property_tests)

  # Exercise real near-limit accounting with both compiled implementations.
  # The accessor is test-local; counters are seeded only before any observers.
  dagflow_add_runtime_unit_test(dagflow_detail_accounting_fault_boundary_property_tests
    tests/detail_accounting_boundary_property_tests.cpp)
  target_compile_definitions(dagflow_detail_accounting_fault_boundary_property_tests
    PRIVATE DAGFLOW_TEST_FAULT_INJECTION DAGFLOW_FUZZ_HOOKS)
  target_link_libraries(dagflow_detail_accounting_fault_boundary_property_tests
    PRIVATE dagflow_test_fault_runtime)
  set_tests_properties(dagflow_detail_accounting_fault_boundary_property_tests
    PROPERTIES LABELS "runtime;property;pbt;stateful;fault-injection")
  add_dependencies(dagflow_model_property_tests
    dagflow_detail_accounting_fault_boundary_property_tests)
  if(DAGFLOW_SANITIZER STREQUAL "thread")
    target_compile_definitions(dagflow_detail_accounting_boundary_property_tests
      PRIVATE DAGFLOW_TEST_SKIP_DEATH_CASES=1)
    target_compile_definitions(dagflow_detail_accounting_fault_boundary_property_tests
      PRIVATE DAGFLOW_TEST_SKIP_DEATH_CASES=1)
  endif()

  # Additional stateful models for queued cancellation, graph range chunks
  # and non-random-access range publication. Retain existing example-based tests.
  dagflow_add_runtime_unit_test(dagflow_remaining_protocol_property_tests
    tests/remaining_protocol_property_tests.cpp)
  target_link_libraries(dagflow_remaining_protocol_property_tests PRIVATE
    ${DAGFLOW_TARGET})
  set_tests_properties(dagflow_remaining_protocol_property_tests PROPERTIES
    LABELS "runtime;property;pbt;stateful")
  add_dependencies(dagflow_model_property_tests
    dagflow_remaining_protocol_property_tests)

  # A test-only parking checkpoint forces publication between prepare() and
  # help(true), without relying on probability or sleeps in production code.
  dagflow_add_runtime_unit_test(dagflow_worker_help_announced_test
    tests/worker_help_announced_test.cpp)
  target_compile_definitions(dagflow_worker_help_announced_test PRIVATE
    DAGFLOW_FUZZ_HOOKS)
  target_link_libraries(dagflow_worker_help_announced_test PRIVATE
    dagflow_test_fault_runtime)
  set_tests_properties(dagflow_worker_help_announced_test PROPERTIES
    LABELS "runtime;adversarial;concurrency;fault-injection")

  dagflow_add_runtime_unit_test(dagflow_graph_cancellation_edge_test
    tests/graph_cancellation_edge_test.cpp)
  target_compile_definitions(dagflow_graph_cancellation_edge_test PRIVATE
    DAGFLOW_FUZZ_HOOKS)
  target_link_libraries(dagflow_graph_cancellation_edge_test PRIVATE
    dagflow_test_fault_runtime)
  set_tests_properties(dagflow_graph_cancellation_edge_test PROPERTIES
    LABELS "runtime;adversarial;concurrency;fault-injection")

  # Bounded lifetime/race regressions; independent of disabled fuzz hooks.
  add_custom_target(dagflow_adversarial_tests)
  add_dependencies(dagflow_adversarial_tests
    dagflow_worker_help_announced_test
    dagflow_graph_cancellation_edge_test)
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
      exception_payload_last_release
      scheduler_contention_protocol
      scheduler_targeted_paths
      deque_last_slot_race)
    dagflow_add_runtime_unit_test(dagflow_${_case}_test tests/${_case}_test.cpp)
    target_link_libraries(dagflow_${_case}_test PRIVATE ${DAGFLOW_TARGET})
    set_tests_properties(dagflow_${_case}_test PROPERTIES LABELS "runtime;adversarial;concurrency")
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
  dagflow_apply_optimization(dagflow_adversarial_runtime)
  if(DAGFLOW_RUNTIME_DIAGNOSTICS)
    target_compile_definitions(dagflow_adversarial_runtime PUBLIC DAGFLOW_RUNTIME_DIAGNOSTICS=1)
  endif()
  # Allocation rollback models use the recording allocator boundary, not the
  # production allocator or the separate protocol-fault runtime.
  foreach(_component IN ITEMS graph_builder storage_api)
    set(_property_target dagflow_${_component}_property_tests)
    dagflow_add_runtime_unit_test(${_property_target}
      tests/${_component}_property_tests.cpp)
    target_link_libraries(${_property_target} PRIVATE dagflow_adversarial_runtime)
    set_tests_properties(${_property_target} PROPERTIES
      LABELS "runtime;property;pbt;stateful;fault-injection")
    add_dependencies(dagflow_model_property_tests ${_property_target})
  endforeach()
  foreach(_case IN ITEMS combine_partial_registration_oom_race range_publication_rollback_lifetime)
    dagflow_add_runtime_unit_test(dagflow_${_case}_test tests/${_case}_test.cpp)
    target_link_libraries(dagflow_${_case}_test PRIVATE dagflow_adversarial_runtime)
    set_tests_properties(dagflow_${_case}_test PROPERTIES LABELS "runtime;adversarial;concurrency")
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

  dagflow_add_runtime_unit_test(dagflow_worker_wait_tests tests/worker_wait_tests.cpp)
  target_link_libraries(dagflow_worker_wait_tests PRIVATE ${DAGFLOW_TARGET})
  set_tests_properties(dagflow_worker_wait_tests PROPERTIES LABELS "runtime;concurrency")

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

  set_property(TEST dagflow_idle_accounting_tests dagflow_scheduler_topology_tests
    dagflow_pool_lifecycle_tests APPEND PROPERTY LABELS concurrency)
endif()

get_property(_dagflow_targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
foreach(_target IN LISTS _dagflow_targets)
  get_target_property(_type ${_target} TYPE)
  if(_type STREQUAL "EXECUTABLE")
    add_dependencies(dagflow_tests ${_target})
  endif()
endforeach()
