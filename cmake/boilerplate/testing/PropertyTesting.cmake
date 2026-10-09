include_guard(GLOBAL)

set(BOILERPLATE_PBT_BACKEND "rapidcheck" CACHE STRING "Property-testing backend")
set_property(CACHE BOILERPLATE_PBT_BACKEND PROPERTY STRINGS rapidcheck)

function(_boilerplate_project_property_testing_finalize)
  get_property(_targets GLOBAL PROPERTY BOILERPLATE_PROPERTY_TEST_TARGETS)
  if(NOT _targets)
    return()
  endif()
  list(REMOVE_DUPLICATES _targets)
  if(NOT TARGET boilerplate_property_tests)
    add_custom_target(boilerplate_property_tests)
  endif()
  add_dependencies(boilerplate_property_tests ${_targets})
  set_property(TARGET boilerplate_property_tests PROPERTY FOLDER "boilerplate/tests")
  if(BUILD_TESTING AND NOT TARGET property-check)
    add_custom_target(property-check
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>"
        -L property --output-on-failure
      DEPENDS boilerplate_property_tests USES_TERMINAL VERBATIM)
    set_property(TARGET property-check PROPERTY FOLDER "boilerplate/tests")
  endif()
endfunction()
