include_guard(GLOBAL)

function(_boilerplate_project_static_analysis_configure)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "Export compile_commands.json for analysis" FORCE)
endfunction()

function(_boilerplate_project_static_analysis_finalize)
  boilerplate_collect_project_sources(_sources EXTENSIONS c cc cpp cxx)
  # A source-tree glob also includes disabled benchmarks and standalone test
  # consumers, which have no compile command or dependencies in this build.
  # Analyze the intersection with sources of the configured targets.
  boilerplate_collect_project_targets(_targets)
  set(_compiled_sources)
  foreach(_target IN LISTS _targets)
    get_target_property(_target_sources ${_target} SOURCES)
    get_target_property(_source_dir ${_target} SOURCE_DIR)
    foreach(_source IN LISTS _target_sources)
      if(_source MATCHES "\\$<")
        continue()
      endif()
      get_filename_component(_source "${_source}" ABSOLUTE BASE_DIR "${_source_dir}")
      if(_source IN_LIST _sources)
        list(APPEND _compiled_sources "${_source}")
      endif()
    endforeach()
  endforeach()
  list(REMOVE_DUPLICATES _compiled_sources)
  set(_sources ${_compiled_sources})
  if(NOT _sources)
    return()
  endif()
  set(_analysis_targets)

  boilerplate_project_tool(_clang_tidy "clang-tidy analysis" NAMES clang-tidy)
  if(_clang_tidy)
    add_custom_target(analyze-clang-tidy
      COMMAND "${_clang_tidy}" -p "${CMAKE_BINARY_DIR}" ${_sources}
      COMMAND_EXPAND_LISTS USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-clang-tidy)
  endif()

  boilerplate_project_tool(_cppcheck "cppcheck analysis" NAMES cppcheck)
  if(_cppcheck)
    add_custom_target(analyze-cppcheck
      COMMAND "${_cppcheck}" --project="${CMAKE_BINARY_DIR}/compile_commands.json"
        --enable=warning,style,performance,portability --inline-suppr --error-exitcode=2
      USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-cppcheck)
  endif()

  boilerplate_project_tool(_iwyu "include-what-you-use analysis" NAMES iwyu_tool.py iwyu_tool)
  if(_iwyu)
    add_custom_target(analyze-iwyu
      COMMAND "${_iwyu}" -p "${CMAKE_BINARY_DIR}"
      USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-iwyu)
  endif()

  boilerplate_project_tool(_clazy "clazy standalone analysis" NAMES clazy-standalone)
  if(_clazy)
    add_custom_target(analyze-clazy
      COMMAND "${_clazy}" -p "${CMAKE_BINARY_DIR}" ${_sources}
      COMMAND_EXPAND_LISTS USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-clazy)
  endif()

  if(_analysis_targets)
    add_custom_target(analyze)
    add_dependencies(analyze ${_analysis_targets})
    set_property(TARGET analyze ${_analysis_targets} PROPERTY FOLDER "boilerplate/quality")
  endif()
endfunction()
