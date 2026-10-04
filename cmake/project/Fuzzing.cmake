include_guard(GLOBAL)

set(DAGFLOW_FUZZ_BACKEND "libfuzzer" CACHE STRING
  "Byte-fuzzing backend for LLVMFuzzerTestOneInput harnesses")
set_property(CACHE DAGFLOW_FUZZ_BACKEND PROPERTY STRINGS libfuzzer aflpp honggfuzz)
set(DAGFLOW_FUZZ_SANITIZER "address-undefined" CACHE STRING
  "Sanitizer used for byte-fuzz targets: none, address, undefined, address-undefined")
set_property(CACHE DAGFLOW_FUZZ_SANITIZER PROPERTY STRINGS none address undefined address-undefined)
set(DAGFLOW_FUZZ_RUNTIME 60 CACHE STRING "Default explicit fuzz campaign duration in seconds")
set(DAGFLOW_FUZZ_SMOKE_LABEL "fuzz" CACHE STRING "CTest label used by fuzz smoke tests")

function(_dagflow_project_fuzzing_finalize)
  get_property(_targets GLOBAL PROPERTY DAGFLOW_FUZZ_TARGETS)
  if(_targets)
    list(REMOVE_DUPLICATES _targets)
    if(NOT TARGET dagflow_fuzzers)
      add_custom_target(dagflow_fuzzers)
    endif()
    add_dependencies(dagflow_fuzzers ${_targets})
    set_property(TARGET dagflow_fuzzers PROPERTY FOLDER "dagflow/fuzz")
  endif()

  get_property(_smoke_tests GLOBAL PROPERTY DAGFLOW_FUZZ_SMOKE_TESTS)
  if(BUILD_TESTING AND _smoke_tests AND NOT TARGET fuzz-smoke)
    add_custom_target(fuzz-smoke
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>"
        -L "${DAGFLOW_FUZZ_SMOKE_LABEL}" --output-on-failure
      DEPENDS dagflow_fuzzers USES_TERMINAL VERBATIM)
    set_property(TARGET fuzz-smoke PROPERTY FOLDER "dagflow/fuzz")
  endif()

  get_property(_run_targets GLOBAL PROPERTY DAGFLOW_FUZZ_RUN_TARGETS)
  if(_run_targets)
    list(REMOVE_DUPLICATES _run_targets)
    # Deliberately no aggregate that starts all campaigns in parallel. A fuzz
    # campaign is an explicit resource-consuming workflow; run fuzz_<target>.
    set_property(TARGET ${_run_targets} PROPERTY FOLDER "dagflow/fuzz")
  endif()

  get_property(_fuzztest_targets GLOBAL PROPERTY DAGFLOW_FUZZTEST_TARGETS)
  if(_fuzztest_targets)
    list(REMOVE_DUPLICATES _fuzztest_targets)
    if(NOT TARGET dagflow_fuzztests)
      add_custom_target(dagflow_fuzztests)
    endif()
    add_dependencies(dagflow_fuzztests ${_fuzztest_targets})
    set_property(TARGET dagflow_fuzztests PROPERTY FOLDER "dagflow/fuzztest")
  endif()
endfunction()
