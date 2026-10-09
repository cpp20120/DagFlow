include_guard(GLOBAL)

function(_boilerplate_project_testing_configure)
  include(CTest)
endfunction()

function(_boilerplate_project_testing_finalize)
  if(NOT BUILD_TESTING)
    return()
  endif()
  if(NOT TARGET boilerplate_tests)
    add_custom_target(boilerplate_tests)
  endif()
  if(NOT TARGET boilerplate_check)
    add_custom_target(boilerplate_check
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${CMAKE_BINARY_DIR}" -C "$<CONFIG>" --output-on-failure
      DEPENDS boilerplate_tests USES_TERMINAL VERBATIM)
  endif()
  if(NOT TARGET check)
    add_custom_target(check DEPENDS boilerplate_check)
  endif()
  set_property(TARGET boilerplate_tests boilerplate_check check PROPERTY FOLDER "boilerplate/tests")
endfunction()
