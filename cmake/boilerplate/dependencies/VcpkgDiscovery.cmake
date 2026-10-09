include_guard(GLOBAL)

# Shared by the pre-project bootstrap and the build matrix. Only inspect known
# locations; never download vcpkg or recursively search the user's drives.
function(boilerplate_find_vcpkg output source_dir)
  set(_explicit "${BOILERPLATE_VCPKG_ROOT}")
  if(NOT _explicit)
    set(_explicit "${VCPKG_ROOT}")
  endif()
  if(NOT _explicit)
    set(_explicit "$ENV{VCPKG_ROOT}")
  endif()
  if(_explicit)
    get_filename_component(_root "${_explicit}" ABSOLUTE BASE_DIR "${source_dir}")
    if(NOT EXISTS "${_root}/scripts/buildsystems/vcpkg.cmake")
      message(FATAL_ERROR "Explicit vcpkg root has no scripts/buildsystems/vcpkg.cmake: ${_root}")
    endif()
    set(${output} "${_root}" PARENT_SCOPE)
    return()
  endif()

  set(_candidates "${source_dir}/vcpkg" "${source_dir}/external/vcpkg"
    "${source_dir}/third_party/vcpkg")
  # Resolve symlinks (e.g. ~/.local/bin/vcpkg) before looking next to the binary.
  find_program(_boilerplate_vcpkg_executable NAMES vcpkg
    PATHS ENV PATH NO_DEFAULT_PATH NO_CACHE NO_CMAKE_FIND_ROOT_PATH)
  if(_boilerplate_vcpkg_executable)
    file(REAL_PATH "${_boilerplate_vcpkg_executable}" _executable)
    get_filename_component(_bin "${_executable}" DIRECTORY)
    get_filename_component(_prefix "${_bin}" DIRECTORY)
    list(APPEND _candidates "${_bin}" "${_prefix}")
  endif()
  foreach(_home IN ITEMS "$ENV{HOME}" "$ENV{USERPROFILE}" "$ENV{LOCALAPPDATA}")
    if(_home)
      list(APPEND _candidates "${_home}/vcpkg")
    endif()
  endforeach()
  if(DEFINED ENV{VSINSTALLDIR} AND NOT "$ENV{VSINSTALLDIR}" STREQUAL "")
    list(APPEND _candidates "$ENV{VSINSTALLDIR}/VC/vcpkg")
  endif()
  list(REMOVE_DUPLICATES _candidates)
  foreach(_candidate IN LISTS _candidates)
    if(EXISTS "${_candidate}/scripts/buildsystems/vcpkg.cmake")
      get_filename_component(_root "${_candidate}" ABSOLUTE BASE_DIR "${source_dir}")
      set(${output} "${_root}" PARENT_SCOPE)
      return()
    endif()
  endforeach()
  message(FATAL_ERROR
    "vcpkg was not found in the project, PATH, user directories or VSINSTALLDIR. "
    "Set VCPKG_ROOT or pass -DBOILERPLATE_VCPKG_ROOT=/path/to/vcpkg. "
    "For a direct configure, CMAKE_TOOLCHAIN_FILE can also select a toolchain.")
endfunction()
