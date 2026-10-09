include_guard(GLOBAL)

function(_boilerplate_project_formatting_finalize)
  boilerplate_collect_project_sources(_sources)
  boilerplate_project_tool(_clang_format "clang-format" NAMES clang-format)
  if(_clang_format AND _sources)
    add_custom_target(format
      COMMAND "${_clang_format}" -i --style=file ${_sources}
      COMMAND_EXPAND_LISTS VERBATIM)
    add_custom_target(format-check
      COMMAND "${_clang_format}" --dry-run --Werror --style=file ${_sources}
      COMMAND_EXPAND_LISTS VERBATIM)
    set_property(TARGET format format-check PROPERTY FOLDER "boilerplate/quality")
  endif()

  boilerplate_collect_project_sources(_cmake_files CMAKE)
  boilerplate_project_tool(_cmake_format "cmake-format" NAMES cmake-format)
  if(_cmake_format AND _cmake_files)
    add_custom_target(format-cmake
      COMMAND "${_cmake_format}" -i ${_cmake_files}
      COMMAND_EXPAND_LISTS VERBATIM)
    add_custom_target(format-cmake-check
      COMMAND "${_cmake_format}" --check ${_cmake_files}
      COMMAND_EXPAND_LISTS VERBATIM)
    set_property(TARGET format-cmake format-cmake-check PROPERTY FOLDER "boilerplate/quality")
  endif()

  if(TARGET format OR TARGET format-cmake)
    add_custom_target(format-all)
    if(TARGET format)
      add_dependencies(format-all format)
    endif()
    if(TARGET format-cmake)
      add_dependencies(format-all format-cmake)
    endif()
  endif()
  if(TARGET format-check OR TARGET format-cmake-check)
    add_custom_target(format-check-all)
    if(TARGET format-check)
      add_dependencies(format-check-all format-check)
    endif()
    if(TARGET format-cmake-check)
      add_dependencies(format-check-all format-cmake-check)
    endif()
  endif()
endfunction()
