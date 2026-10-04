include_guard(GLOBAL)

function(_dagflow_project_testing_configure)
  include(CTest)
endfunction()

function(_dagflow_project_testing_finalize)
  if(NOT BUILD_TESTING)
    return()
  endif()
  if(NOT TARGET dagflow_tests)
    add_custom_target(dagflow_tests)
  endif()
  if(NOT TARGET dagflow_check)
    add_custom_target(dagflow_check
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>" --output-on-failure
      DEPENDS dagflow_tests USES_TERMINAL VERBATIM)
  endif()
  if(NOT TARGET check)
    add_custom_target(check DEPENDS dagflow_check)
  endif()
  set_property(TARGET dagflow_tests dagflow_check check PROPERTY FOLDER "dagflow/tests")
endfunction()
