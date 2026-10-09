# Each executable has one main(). All build profiles go through the same rules.
add_custom_target(dagflow_benchmarks)
function(dagflow_add_runtime_benchmark target output source)
  boilerplate_add_executable(${target} SOURCES ${source})
  boilerplate_set_output_name(${target} ${output})
  add_dependencies(dagflow_benchmarks ${target})
endfunction()

# Explicit run targets accept per-benchmark arguments; they are never in ALL.
function(dagflow_add_benchmark_run target stem default_args)
  if(NOT TARGET ${target})
    return()
  endif()
  string(TOUPPER "${stem}" upper_stem)
  set(variable "DAGFLOW_${upper_stem}_ARGS")
  set(${variable} "${default_args}" CACHE STRING "Arguments for dagflow_run_${stem}")
  separate_arguments(arguments NATIVE_COMMAND "${${variable}}")
  boilerplate_add_scenario(${stem} TARGET ${target} GROUP dagflow
    ARGS ${arguments} REPEATS 1 WARMUP 0)
  add_custom_target(dagflow_run_${stem} DEPENDS run_${stem})
endfunction()

function(dagflow_add_benchmark_smoke_tests)
  if(TARGET dagflow_runtime_bench)
    add_test(NAME dagflow_runtime_bench_smoke COMMAND dagflow_runtime_bench 2 65 1)
    set_tests_properties(dagflow_runtime_bench_smoke PROPERTIES LABELS benchmark TIMEOUT 30)
  endif()
  if(TARGET dagflow_runtime_suite)
    add_test(NAME dagflow_runtime_suite_smoke COMMAND dagflow_runtime_suite
      --workers 2 --tasks 65 --iterations 3 --repeats 2 --warmup 0 --fresh-graph)
    set_tests_properties(dagflow_runtime_suite_smoke PROPERTIES LABELS benchmark TIMEOUT 30)
  endif()
  if(TARGET dagflow_stress_bench)
    add_test(NAME dagflow_stress_bench_smoke COMMAND dagflow_stress_bench
      --workers 2 --tasks 128 --warmup 0 --warmup-ms 0 --repeats 1
      --bursts 2 --idle-us 100 --verify exact)
    set_tests_properties(dagflow_stress_bench_smoke PROPERTIES LABELS benchmark TIMEOUT 30)
  endif()
  foreach(target IN ITEMS dagflow_public_api_bench dagflow_tbb_bench)
    if(TARGET ${target})
      add_test(NAME ${target}_smoke COMMAND ${target}
        --benchmark chain --workers 2 --runs 1 --warmup 0 --payload-rounds 1)
      set_tests_properties(${target}_smoke PROPERTIES LABELS benchmark TIMEOUT 60)
    endif()
  endforeach()
endfunction()

if((DAGFLOW_BUILD_STRESS_BENCH OR DAGFLOW_BUILD_RUNTIME_BENCH OR DAGFLOW_BUILD_RUNTIME_SUITE OR DAGFLOW_BUILD_PUBLIC_API_BENCH OR
    DAGFLOW_BUILD_FUNCTION_BENCH) AND NOT DAGFLOW_TARGET)
  message(FATAL_ERROR "DagFlow benchmarks require DAGFLOW_BUILD_STATIC or DAGFLOW_BUILD_SHARED")
endif()

if(DAGFLOW_BUILD_STRESS_BENCH)
  dagflow_add_runtime_benchmark(dagflow_stress_bench dagflow-stress-bench bench/stress_harness.cpp)
  target_link_libraries(dagflow_stress_bench PRIVATE ${DAGFLOW_TARGET})
endif()

if(DAGFLOW_BUILD_RUNTIME_BENCH)
  dagflow_add_runtime_benchmark(dagflow_runtime_bench dagflow-runtime-bench bench/runtime_bench.cpp)
  target_link_libraries(dagflow_runtime_bench PRIVATE ${DAGFLOW_TARGET})
endif()

if(DAGFLOW_BUILD_RUNTIME_SUITE)
  dagflow_add_runtime_benchmark(dagflow_runtime_suite dagflow-runtime-suite bench/runtime_suite.cpp)
  target_link_libraries(dagflow_runtime_suite PRIVATE ${DAGFLOW_TARGET})
endif()
if(DAGFLOW_BUILD_PUBLIC_API_BENCH)
  set(DAGFLOW_PUBLIC_API_SOURCE "${PROJECT_SOURCE_DIR}/bench/public_api_bench.cpp"
      CACHE FILEPATH "Public API benchmark source")
  dagflow_add_runtime_benchmark(dagflow_public_api_bench dagflow-public-api-bench "${DAGFLOW_PUBLIC_API_SOURCE}")
  target_link_libraries(dagflow_public_api_bench PRIVATE ${DAGFLOW_TARGET})
endif()
if(DAGFLOW_BUILD_FUNCTION_BENCH)
  dagflow_add_runtime_benchmark(dagflow_function_bench dagflow-function-bench bench/function_bench.cpp)
  target_link_libraries(dagflow_function_bench PRIVATE ${DAGFLOW_TARGET})
endif()
if(DAGFLOW_BUILD_BENCH)
  find_package(TBB REQUIRED COMPONENTS tbb)
  set(DAGFLOW_TBB_BENCH_SOURCE "${PROJECT_SOURCE_DIR}/bench/tbb_bench.cpp"
      CACHE FILEPATH "oneTBB public API benchmark source")
  dagflow_add_runtime_benchmark(dagflow_tbb_bench dagflow-tbb-bench "${DAGFLOW_TBB_BENCH_SOURCE}")
  target_link_libraries(dagflow_tbb_bench PRIVATE TBB::tbb Threads::Threads)
endif()

if(DAGFLOW_BUILD_GITHUB_BENCH)
  set(DAGFLOW_GITHUB_SOURCE_DIR ""
      CACHE PATH "Explicit path to an external GitHub snapshot for comparison")
  if(NOT EXISTS "${DAGFLOW_GITHUB_SOURCE_DIR}/src/thread_pool.cpp")
    message(FATAL_ERROR
      "Set DAGFLOW_GITHUB_SOURCE_DIR to a snapshot containing src/thread_pool.cpp; legacy sources are not bundled")
  endif()
  # The snapshot's CMake lists a missing qsbr header. Use its actual translation
  # unit and headers; no runtime patches or substitution with current sources.
  add_library(dagflow_github_runtime STATIC "${DAGFLOW_GITHUB_SOURCE_DIR}/src/thread_pool.cpp")
  target_include_directories(dagflow_github_runtime PUBLIC "${DAGFLOW_GITHUB_SOURCE_DIR}/include")
  target_compile_definitions(dagflow_github_runtime PRIVATE DAGFLOW_STATIC)
  target_link_libraries(dagflow_github_runtime PUBLIC Threads::Threads)
  boilerplate_apply_target_policy(dagflow_github_runtime)
  dagflow_add_runtime_benchmark(dagflow_github_suite dagflow-github-suite bench/runtime_suite.cpp)
  target_compile_definitions(dagflow_github_suite PRIVATE DAGFLOW_SUITE_LEGACY)
  target_link_libraries(dagflow_github_suite PRIVATE dagflow_github_runtime)
endif()

dagflow_add_benchmark_run(dagflow_runtime_bench runtime_bench "4 4096 9")
dagflow_add_benchmark_run(dagflow_runtime_suite runtime_suite
  "--workers 4 --tasks 4096 --iterations 0 --repeats 9 --warmup 2")
dagflow_add_benchmark_run(dagflow_stress_bench stress_bench
  "--workers 4 --tasks 4096 --repeats 9 --json")
dagflow_add_benchmark_run(dagflow_public_api_bench public_api_bench
  "--benchmark noop --workers 4 --runs 5 --warmup 1")
dagflow_add_benchmark_run(dagflow_tbb_bench tbb_bench
  "--benchmark noop --workers 4 --runs 5 --warmup 1")
dagflow_add_benchmark_run(dagflow_function_bench function_bench "")
dagflow_add_benchmark_run(dagflow_github_suite github_suite
  "--scenario external_handles --workers 4 --tasks 4096 --iterations 0 --repeats 9 --warmup 2")
