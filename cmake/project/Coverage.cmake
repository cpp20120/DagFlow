include_guard(GLOBAL)
set(DAGFLOW_COVERAGE_DIR "${CMAKE_BINARY_DIR}/coverage" CACHE PATH "Coverage report output directory")
set(DAGFLOW_COVERAGE_GCOV_TOOL "" CACHE STRING
  "Optional gcov command as a CMake list; empty selects a tool for the compiler")
set(DAGFLOW_COVERAGE_EXCLUDES "/usr/*;*/tests/*;*/test/*;*/_deps/*" CACHE STRING
  "Coverage source exclusions, applied before parsing external coverage data")
set(_dagflow_coverage_functions_default ON)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # LLVM's gcov compatibility format can put a function's start on a line with
  # no executable code. Recent genhtml cannot categorize such function records.
  set(_dagflow_coverage_functions_default OFF)
endif()
option(DAGFLOW_COVERAGE_FUNCTIONS "Include function coverage in the report"
  ${_dagflow_coverage_functions_default})
unset(_dagflow_coverage_functions_default)

function(_dagflow_project_coverage_finalize)
  if(NOT BUILD_TESTING)
    message(STATUS "DagFlow: coverage-report enabled but BUILD_TESTING=OFF")
    return()
  endif()
  dagflow_project_tool(_lcov "lcov coverage reports" NAMES lcov)
  dagflow_project_tool(_genhtml "genhtml coverage reports" NAMES genhtml)
  if(NOT _lcov OR NOT _genhtml)
    return()
  endif()
  set(_gcov_tool ${DAGFLOW_COVERAGE_GCOV_TOOL})
  if(NOT _gcov_tool AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    get_filename_component(_compiler_dir "${CMAKE_CXX_COMPILER}" DIRECTORY)
    string(REGEX MATCH "^[0-9]+" _compiler_major "${CMAKE_CXX_COMPILER_VERSION}")
    find_program(_llvm_cov NAMES "llvm-cov-${_compiler_major}" llvm-cov
      HINTS "${_compiler_dir}" NO_CACHE)
    if(NOT _llvm_cov)
      message(FATAL_ERROR "Clang coverage-report requires llvm-cov or DAGFLOW_COVERAGE_GCOV_TOOL")
    endif()
    set(_gcov_tool "${_llvm_cov}" gcov)
  endif()
  set(_gcov_args)
  foreach(_arg IN LISTS _gcov_tool)
    list(APPEND _gcov_args --gcov-tool "${_arg}")
  endforeach()
  set(_format_args)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # llvm-cov's gcov format has no function end positions. lcov's heuristic
    # cannot reliably reconstruct them for C++ inline functions/destructors.
    set(_format_args --rc derive_function_end_line=0)
  endif()
  if(NOT DAGFLOW_COVERAGE_FUNCTIONS)
    list(APPEND _format_args --no-function-coverage)
  endif()
  set(_exclude_args)
  foreach(_pattern IN LISTS DAGFLOW_COVERAGE_EXCLUDES)
    list(APPEND _exclude_args --exclude "${_pattern}")
  endforeach()
  file(MAKE_DIRECTORY "${DAGFLOW_COVERAGE_DIR}")
  add_custom_target(coverage-report
    COMMAND "${_lcov}" --directory "${CMAKE_BINARY_DIR}" --zerocounters
    COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>" --output-on-failure
    COMMAND "${_lcov}" ${_gcov_args} ${_format_args} ${_exclude_args} --ignore-errors unused
      --directory "${CMAKE_BINARY_DIR}" --capture
      --output-file "${DAGFLOW_COVERAGE_DIR}/coverage.info"
    COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${DAGFLOW_COVERAGE_DIR}/coverage.info"
      "${DAGFLOW_COVERAGE_DIR}/filtered.info"
    COMMAND "${_genhtml}" ${_format_args} "${DAGFLOW_COVERAGE_DIR}/filtered.info"
      --output-directory "${DAGFLOW_COVERAGE_DIR}/html"
    DEPENDS dagflow_tests USES_TERMINAL VERBATIM)
  set_property(TARGET coverage-report PROPERTY FOLDER "dagflow/quality")
endfunction()
