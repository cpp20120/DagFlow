include_guard(GLOBAL)

set(BOILERPLATE_FUZZ_BACKEND "libfuzzer" CACHE STRING
  "Byte-fuzzing backend for LLVMFuzzerTestOneInput harnesses")
set_property(CACHE BOILERPLATE_FUZZ_BACKEND PROPERTY STRINGS libfuzzer aflpp honggfuzz)
set(BOILERPLATE_FUZZ_SANITIZER "address-undefined" CACHE STRING
  "Sanitizer used for byte-fuzz targets: none, address, undefined, address-undefined")
set_property(CACHE BOILERPLATE_FUZZ_SANITIZER PROPERTY STRINGS none address undefined address-undefined)
set(BOILERPLATE_FUZZ_RUNTIME 60 CACHE STRING "Default explicit fuzz campaign duration in seconds")
set(BOILERPLATE_FUZZ_TIMEOUT 15 CACHE STRING "libFuzzer timeout per input in seconds")
set(BOILERPLATE_FUZZ_SMOKE_LABEL "fuzz" CACHE STRING "CTest label used by fuzz smoke tests")

function(_boilerplate_project_fuzzing_finalize)
  get_property(_targets GLOBAL PROPERTY BOILERPLATE_FUZZ_TARGETS)
  if(_targets)
    list(REMOVE_DUPLICATES _targets)
    if(NOT TARGET boilerplate_fuzzers)
      add_custom_target(boilerplate_fuzzers)
    endif()
    add_dependencies(boilerplate_fuzzers ${_targets})
    set_property(TARGET boilerplate_fuzzers PROPERTY FOLDER "boilerplate/fuzz")
  endif()

  get_property(_smoke_tests GLOBAL PROPERTY BOILERPLATE_FUZZ_SMOKE_TESTS)
  if(BUILD_TESTING AND _smoke_tests AND NOT TARGET fuzz-smoke)
    add_custom_target(fuzz-smoke
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>"
        -L "${BOILERPLATE_FUZZ_SMOKE_LABEL}" --output-on-failure
      DEPENDS boilerplate_fuzzers USES_TERMINAL VERBATIM)
    set_property(TARGET fuzz-smoke PROPERTY FOLDER "boilerplate/fuzz")
  endif()

  get_property(_run_targets GLOBAL PROPERTY BOILERPLATE_FUZZ_RUN_TARGETS)
  if(_run_targets)
    list(REMOVE_DUPLICATES _run_targets)
    # Deliberately no aggregate that starts all campaigns in parallel. A fuzz
    # campaign is an explicit resource-consuming workflow; run fuzz_<target>.
    set_property(TARGET ${_run_targets} PROPERTY FOLDER "boilerplate/fuzz")
  endif()

  get_property(_fuzztest_targets GLOBAL PROPERTY BOILERPLATE_FUZZTEST_TARGETS)
  if(_fuzztest_targets)
    list(REMOVE_DUPLICATES _fuzztest_targets)
    if(NOT TARGET boilerplate_fuzztests)
      add_custom_target(boilerplate_fuzztests)
    endif()
    add_dependencies(boilerplate_fuzztests ${_fuzztest_targets})
    set_property(TARGET boilerplate_fuzztests PROPERTY FOLDER "boilerplate/fuzztest")
  endif()
endfunction()
