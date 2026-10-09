# Private instrumented runtime: production shared/static targets stay untouched.
dagflow_add_fuzz_library(dagflow_fuzz_runtime
  SOURCES ${DAGFLOW_RUNTIME_SOURCES} LIBRARIES Threads::Threads)
target_include_directories(dagflow_fuzz_runtime PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_compile_features(dagflow_fuzz_runtime PUBLIC cxx_std_23)
target_compile_definitions(dagflow_fuzz_runtime PRIVATE DAGFLOW_STATIC)
dagflow_apply_allocator(dagflow_fuzz_runtime)
target_compile_definitions(dagflow_fuzz_runtime PRIVATE DAGFLOW_FUZZ_HOOKS=1)
if(DAGFLOW_RUNTIME_DIAGNOSTICS)
  target_compile_definitions(dagflow_fuzz_runtime PUBLIC DAGFLOW_RUNTIME_DIAGNOSTICS=1)
endif()

set(DAGFLOW_FUZZ_LAYERS "containers;function;queues;scope;pool;graph;parking;lifecycle;accounting;scheduler;credits;allocation;exceptions;ranges;graph_scope;joins;saturation;scope_races" CACHE STRING
  "Fuzz layers to build (CMake list; defaults to 18 active layers)")
if(NOT DAGFLOW_FUZZ_LAYERS)
  message(FATAL_ERROR "DAGFLOW_FUZZ_LAYERS must contain at least one layer")
endif()
set(_dagflow_fuzz_layers ${DAGFLOW_FUZZ_LAYERS})
list(REMOVE_DUPLICATES _dagflow_fuzz_layers)
foreach(layer IN LISTS _dagflow_fuzz_layers)
  if(layer STREQUAL "wake_protocol")
    message(FATAL_ERROR
      "wake_protocol is disabled while DAGFLOW_FUZZ_POINT calls are commented out. "
      "Remove it from DAGFLOW_FUZZ_LAYERS or use -U DAGFLOW_FUZZ_LAYERS to restore active defaults.")
  endif()
  if(NOT layer MATCHES "^(containers|function|queues|scope|pool|graph|parking|lifecycle|accounting|scheduler|credits|allocation|exceptions|ranges|graph_scope|joins|saturation|scope_races)$")
    message(FATAL_ERROR "Unknown DAGFLOW_FUZZ_LAYERS entry: ${layer}")
  endif()
  dagflow_add_fuzzer(dagflow_fuzz_${layer}
    SOURCES "fuzz/${layer}_fuzz.cpp" LIBRARIES dagflow_fuzz_runtime
    SEED_CORPUS "fuzz/corpus/${layer}" MAX_LEN 4096)
  target_compile_definitions(dagflow_fuzz_${layer} PRIVATE DAGFLOW_FUZZ_HOOKS=1)
  if(BUILD_TESTING AND layer MATCHES "^(saturation|scope_races)$")
    # Replay includes all deterministic seeds, some with real 16K-slot queues.
    # This is a corpus-wide allowance; the per-input fuzz timeout is unchanged.
    set_tests_properties(dagflow_fuzz_${layer} PROPERTIES TIMEOUT 120)
  endif()
endforeach()
