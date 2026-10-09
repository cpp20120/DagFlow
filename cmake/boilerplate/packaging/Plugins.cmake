include_guard(GLOBAL)
include(GenerateExportHeader)

# A plugin is loaded explicitly, never linked into its host. The ABI belongs to
# the application; see examples/plugins for a versioned C entry-point contract.
function(boilerplate_add_plugin target)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "" "SOURCES;LIBRARIES;INCLUDE_DIRS;POLICIES;POLICY_OPTIONS")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES OR NOT ARG_SOURCES)
    message(FATAL_ERROR "boilerplate_add_plugin(${target}): SOURCES required; invalid arguments")
  endif()
  add_library(${target} MODULE ${ARG_SOURCES})
  target_link_libraries(${target} PRIVATE ${ARG_LIBRARIES})
  string(MAKE_C_IDENTIFIER "${target}" _prefix)
  string(TOUPPER "${_prefix}" _prefix)
  set(_generated "${CMAKE_CURRENT_BINARY_DIR}/generated/${target}")
  file(MAKE_DIRECTORY "${_generated}")
  set_target_properties(${target} PROPERTIES PREFIX "" POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN ON DEFINE_SYMBOL "${_prefix}_EXPORTS")
  generate_export_header(${target} BASE_NAME "${_prefix}"
    EXPORT_FILE_NAME "${_generated}/${target}_export.h" EXPORT_MACRO_NAME "${_prefix}_EXPORT")
  target_include_directories(${target} PRIVATE "${_generated}" ${ARG_INCLUDE_DIRS})
  set(_policy_args ${ARG_POLICY_OPTIONS})
  if(ARG_POLICIES)
    list(PREPEND _policy_args POLICIES ${ARG_POLICIES})
  endif()
  boilerplate_apply_optimization(${target} ${_policy_args})
  boilerplate_set_output_name(${target} "${target}")
  if(WIN32)
    add_custom_command(TARGET ${target} POST_BUILD
      COMMAND "${CMAKE_COMMAND}" "-DRUNTIME_FILES=$<TARGET_RUNTIME_DLLS:${target}>"
        "-DDESTINATION=$<TARGET_FILE_DIR:${target}>"
        -P "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/CopyRuntimeLibraries.cmake" VERBATIM)
  endif()
  boilerplate_get_target_setting(${target} CFI _cfi)
  boilerplate_get_target_setting(${target} CFI_DIAGNOSTICS _diagnostics)
  if(NOT _cfi STREQUAL "none" AND _diagnostics)
    message(FATAL_ERROR "Plugin ${target}: diagnostic CFI requires a host-owned UBSan runtime; use trap mode or configure a native MODULE target explicitly")
  endif()
endfunction()
