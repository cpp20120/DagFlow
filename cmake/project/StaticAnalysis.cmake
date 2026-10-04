include_guard(GLOBAL)

function(_dagflow_project_static_analysis_configure)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "Export compile_commands.json for analysis" FORCE)
endfunction()

function(_dagflow_project_static_analysis_finalize)
  dagflow_collect_project_sources(_sources EXTENSIONS c cc cpp cxx)
  if(NOT _sources)
    return()
  endif()
  set(_analysis_targets)

  dagflow_project_tool(_clang_tidy "clang-tidy analysis" NAMES clang-tidy)
  if(_clang_tidy)
    add_custom_target(analyze-clang-tidy
      COMMAND "${_clang_tidy}" -p "${CMAKE_BINARY_DIR}" ${_sources}
      COMMAND_EXPAND_LISTS USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-clang-tidy)
  endif()

  dagflow_project_tool(_cppcheck "cppcheck analysis" NAMES cppcheck)
  if(_cppcheck)
    add_custom_target(analyze-cppcheck
      COMMAND "${_cppcheck}" --project="${CMAKE_BINARY_DIR}/compile_commands.json"
        --enable=warning,style,performance,portability --inline-suppr --error-exitcode=2
      USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-cppcheck)
  endif()

  dagflow_project_tool(_iwyu "include-what-you-use analysis" NAMES iwyu_tool.py iwyu_tool)
  if(_iwyu)
    add_custom_target(analyze-iwyu
      COMMAND "${_iwyu}" -p "${CMAKE_BINARY_DIR}"
      USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-iwyu)
  endif()

  dagflow_project_tool(_clazy "clazy standalone analysis" NAMES clazy-standalone)
  if(_clazy)
    add_custom_target(analyze-clazy
      COMMAND "${_clazy}" -p "${CMAKE_BINARY_DIR}" ${_sources}
      COMMAND_EXPAND_LISTS USES_TERMINAL VERBATIM)
    list(APPEND _analysis_targets analyze-clazy)
  endif()

  if(_analysis_targets)
    add_custom_target(analyze)
    add_dependencies(analyze ${_analysis_targets})
    set_property(TARGET analyze ${_analysis_targets} PROPERTY FOLDER "dagflow/quality")
  endif()
endfunction()
