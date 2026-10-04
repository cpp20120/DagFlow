include_guard(GLOBAL)
set(DAGFLOW_COVERAGE_DIR "${CMAKE_BINARY_DIR}/coverage" CACHE PATH "Coverage report output directory")

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
  file(MAKE_DIRECTORY "${DAGFLOW_COVERAGE_DIR}")
  add_custom_target(coverage-report
    COMMAND "${_lcov}" --directory "${CMAKE_BINARY_DIR}" --zerocounters
    COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>" --output-on-failure
    COMMAND "${_lcov}" --directory "${CMAKE_BINARY_DIR}" --capture
      --output-file "${DAGFLOW_COVERAGE_DIR}/coverage.info"
    COMMAND "${_lcov}" --remove "${DAGFLOW_COVERAGE_DIR}/coverage.info"
      "/usr/*" "*/tests/*" "*/test/*" "*/_deps/*"
      --output-file "${DAGFLOW_COVERAGE_DIR}/filtered.info"
    COMMAND "${_genhtml}" "${DAGFLOW_COVERAGE_DIR}/filtered.info"
      --output-directory "${DAGFLOW_COVERAGE_DIR}/html"
    DEPENDS dagflow_tests USES_TERMINAL VERBATIM)
  set_property(TARGET coverage-report PROPERTY FOLDER "dagflow/quality")
endfunction()
