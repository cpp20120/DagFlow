include_guard(GLOBAL)

set(DAGFLOW_PBT_BACKEND "rapidcheck" CACHE STRING "Property-testing backend")
set_property(CACHE DAGFLOW_PBT_BACKEND PROPERTY STRINGS rapidcheck)

function(_dagflow_project_property_testing_finalize)
  get_property(_targets GLOBAL PROPERTY DAGFLOW_PROPERTY_TEST_TARGETS)
  if(NOT _targets)
    return()
  endif()
  list(REMOVE_DUPLICATES _targets)
  if(NOT TARGET dagflow_property_tests)
    add_custom_target(dagflow_property_tests)
  endif()
  add_dependencies(dagflow_property_tests ${_targets})
  set_property(TARGET dagflow_property_tests PROPERTY FOLDER "dagflow/tests")
  if(BUILD_TESTING AND NOT TARGET property-check)
    add_custom_target(property-check
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>"
        -L property --output-on-failure
      DEPENDS dagflow_property_tests USES_TERMINAL VERBATIM)
    set_property(TARGET property-check PROPERTY FOLDER "dagflow/tests")
  endif()
endfunction()
