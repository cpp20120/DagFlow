include_guard(GLOBAL)
include("${CMAKE_CURRENT_LIST_DIR}/dependencies/VcpkgDiscovery.cmake")

# Pre-project bootstrap. Include this module before project() when a dependency
# provider needs to select a toolchain (notably vcpkg). Ordinary/system builds
# can call it too; it deliberately does not select a compiler or generator.
set(BOILERPLATE_DEPENDENCY_PROVIDER "system" CACHE STRING
  "Dependency provider: none, system, vcpkg, fetchcontent, or cpm")
set_property(CACHE BOILERPLATE_DEPENDENCY_PROVIDER PROPERTY STRINGS none system vcpkg fetchcontent cpm)
set(BOILERPLATE_VCPKG_ROOT "" CACHE PATH "Explicit vcpkg root; empty enables automatic discovery")
option(BOILERPLATE_ALLOW_IN_SOURCE_BUILD "Allow configuring directly in the source tree" OFF)

function(boilerplate_prevent_in_source_build)
  if(BOILERPLATE_ALLOW_IN_SOURCE_BUILD)
    return()
  endif()
  file(REAL_PATH "${CMAKE_SOURCE_DIR}" _source)
  file(REAL_PATH "${CMAKE_BINARY_DIR}" _binary)
  if(_source STREQUAL _binary)
    message(FATAL_ERROR
      "In-source builds are disabled. Use e.g. cmake -S . -B out/build/dev. "
      "Delete CMakeCache.txt/CMakeFiles from the source tree before retrying.")
  endif()
endfunction()

function(boilerplate_bootstrap)
  if(NOT BOILERPLATE_DEPENDENCY_PROVIDER MATCHES "^(none|system|vcpkg|fetchcontent|cpm)$")
    message(FATAL_ERROR "Unknown BOILERPLATE_DEPENDENCY_PROVIDER=${BOILERPLATE_DEPENDENCY_PROVIDER}")
  endif()
  boilerplate_prevent_in_source_build()

  if(BOILERPLATE_DEPENDENCY_PROVIDER STREQUAL "vcpkg")
    if(CMAKE_TOOLCHAIN_FILE OR NOT "$ENV{CMAKE_TOOLCHAIN_FILE}" STREQUAL "")
      return()
    endif()
    boilerplate_find_vcpkg(_root "${CMAKE_SOURCE_DIR}")
    set(_toolchain "${_root}/scripts/buildsystems/vcpkg.cmake")
    set(CMAKE_TOOLCHAIN_FILE "${_toolchain}" CACHE FILEPATH "vcpkg toolchain selected by boilerplate")
    message(STATUS "Boilerplate: vcpkg root=${_root}")
  endif()
endfunction()
