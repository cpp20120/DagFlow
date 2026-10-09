include_guard(GLOBAL)
option(DAGFLOW_FETCH_DEPENDENCIES "Allow explicit pinned FetchContent fallback if packages are absent" OFF)
set(DAGFLOW_CPM_FILE "" CACHE FILEPATH "Optional CPM.cmake file used when DAGFLOW_DEPENDENCY_PROVIDER=cpm")

# Resolve a package without making a dependency manager part of target/domain APIs.
# system/vcpkg consume installed CMake packages; fetchcontent/cpm first try the
# installed package and then use the explicit pinned repository supplied by the caller.
function(dagflow_require_dependency)
  cmake_parse_arguments(PARSE_ARGV 0 ARG "NO_SUBMODULES" "NAME;PACKAGE;TARGET;VERSION;GIT_REPOSITORY;GIT_TAG" "COMPONENTS;OPTIONS")
  if(ARG_UNPARSED_ARGUMENTS OR NOT ARG_NAME OR NOT ARG_PACKAGE OR NOT ARG_TARGET)
    message(FATAL_ERROR "dagflow_require_dependency requires NAME, PACKAGE and TARGET")
  endif()
  if(TARGET ${ARG_TARGET})
    return()
  endif()

  set(_find_args)
  if(ARG_VERSION)
    list(APPEND _find_args "${ARG_VERSION}")
  endif()
  list(APPEND _find_args CONFIG QUIET)
  if(ARG_COMPONENTS)
    list(APPEND _find_args COMPONENTS ${ARG_COMPONENTS})
  endif()
  find_package(${ARG_PACKAGE} ${_find_args})
  if(TARGET ${ARG_TARGET})
    return()
  endif()

  if(DAGFLOW_DEPENDENCY_PROVIDER MATCHES "^(none|system|vcpkg)$")
    message(FATAL_ERROR
      "Dependency ${ARG_PACKAGE} did not provide ${ARG_TARGET}. Install it for provider "
      "'${DAGFLOW_DEPENDENCY_PROVIDER}' or select an explicit fetch provider.")
  endif()
  if(NOT ARG_GIT_REPOSITORY OR NOT ARG_GIT_TAG)
    message(FATAL_ERROR "Dependency ${ARG_NAME} needs GIT_REPOSITORY and pinned GIT_TAG for fetch fallback")
  endif()

  foreach(_option IN LISTS ARG_OPTIONS)
    string(REPLACE "=" ";" _parts "${_option}")
    list(LENGTH _parts _count)
    if(_count GREATER_EQUAL 2)
      list(GET _parts 0 _key)
      list(REMOVE_AT _parts 0)
      list(JOIN _parts "=" _value)
      set(${_key} "${_value}" CACHE STRING "dependency option" FORCE)
    endif()
  endforeach()

  if(DAGFLOW_DEPENDENCY_PROVIDER STREQUAL "fetchcontent")
    include(FetchContent)
    # A depth-one clone only contains branch tips and cannot check out an
    # arbitrary pinned commit (RapidCheck uses a full commit hash).
    set(_shallow TRUE)
    string(LENGTH "${ARG_GIT_TAG}" _revision_length)
    if(_revision_length EQUAL 40 AND ARG_GIT_TAG MATCHES "^[0-9a-fA-F]+$")
      set(_shallow FALSE)
    endif()
    set(_download_args
      GIT_REPOSITORY "${ARG_GIT_REPOSITORY}"
      GIT_TAG "${ARG_GIT_TAG}"
      GIT_SHALLOW ${_shallow})
    if(ARG_NO_SUBMODULES)
      FetchContent_Declare(${ARG_NAME} ${_download_args} GIT_SUBMODULES "")
    else()
      FetchContent_Declare(${ARG_NAME} ${_download_args})
    endif()
    FetchContent_MakeAvailable(${ARG_NAME})
  elseif(DAGFLOW_DEPENDENCY_PROVIDER STREQUAL "cpm")
    if(NOT COMMAND CPMAddPackage)
      if(DAGFLOW_CPM_FILE AND EXISTS "${DAGFLOW_CPM_FILE}")
        include("${DAGFLOW_CPM_FILE}")
      else()
        message(FATAL_ERROR "CPM provider requires CPMAddPackage or DAGFLOW_CPM_FILE")
      endif()
    endif()
    CPMAddPackage(NAME ${ARG_NAME}
      GIT_REPOSITORY "${ARG_GIT_REPOSITORY}"
      GIT_TAG "${ARG_GIT_TAG}"
      OPTIONS ${ARG_OPTIONS})
  else()
    message(FATAL_ERROR "Unsupported dependency provider: ${DAGFLOW_DEPENDENCY_PROVIDER}")
  endif()

  if(NOT TARGET ${ARG_TARGET})
    message(FATAL_ERROR "Dependency ${ARG_NAME} resolved but expected target ${ARG_TARGET} is missing")
  endif()
endfunction()

set(DAGFLOW_RAPIDCHECK_TAG ff6af6fc683159deb51c543b065eba14dfcf329b CACHE STRING
  "Pinned RapidCheck revision used by explicit fetch fallback")
set(DAGFLOW_RAPIDCHECK_SOURCE_DIR "" CACHE PATH
  "Existing RapidCheck source tree; preferred over downloading it")

function(dagflow_require_rapidcheck)
  if(TARGET rapidcheck AND TARGET rapidcheck_gtest)
    return()
  endif()
  # vcpkg installs RapidCheck's extras (including rapidcheck_gtest). For an
  # explicit source fallback request the same modules before adding the project.
  set(RC_ENABLE_GTEST ON CACHE BOOL "RapidCheck GoogleTest integration" FORCE)
  set(RC_INSTALL_ALL_EXTRAS ON CACHE BOOL "RapidCheck extras" FORCE)
  set(RC_ENABLE_TESTS OFF CACHE BOOL "RapidCheck upstream tests" FORCE)
  if(DAGFLOW_RAPIDCHECK_SOURCE_DIR)
    if(NOT EXISTS "${DAGFLOW_RAPIDCHECK_SOURCE_DIR}/CMakeLists.txt")
      message(FATAL_ERROR "DAGFLOW_RAPIDCHECK_SOURCE_DIR has no CMakeLists.txt: ${DAGFLOW_RAPIDCHECK_SOURCE_DIR}")
    endif()
    add_subdirectory("${DAGFLOW_RAPIDCHECK_SOURCE_DIR}"
      "${CMAKE_BINARY_DIR}/_deps/rapidcheck-build" EXCLUDE_FROM_ALL)
  else()
    dagflow_require_dependency(
      NAME rapidcheck PACKAGE rapidcheck TARGET rapidcheck
      NO_SUBMODULES
      GIT_REPOSITORY https://github.com/emil-e/rapidcheck.git
      GIT_TAG "${DAGFLOW_RAPIDCHECK_TAG}"
      OPTIONS "RC_ENABLE_GTEST=ON" "RC_INSTALL_ALL_EXTRAS=ON" "RC_ENABLE_TESTS=OFF")
  endif()
  if(NOT TARGET rapidcheck_gtest)
    message(FATAL_ERROR
      "RapidCheck resolved but rapidcheck_gtest is missing. The package must be built with RC_ENABLE_GTEST/RC_INSTALL_ALL_EXTRAS.")
  endif()
endfunction()

set(DAGFLOW_GOOGLETEST_TAG v1.17.0 CACHE STRING "Pinned GoogleTest revision")

function(dagflow_require_googletest)
  if(TARGET GTest::gtest_main)
    return()
  endif()
  if(DAGFLOW_FETCH_DEPENDENCIES AND DAGFLOW_DEPENDENCY_PROVIDER MATCHES "^(none|system|vcpkg)$")
    set(DAGFLOW_DEPENDENCY_PROVIDER fetchcontent)
  endif()
  dagflow_require_dependency(
    NAME googletest PACKAGE GTest TARGET GTest::gtest_main
    GIT_REPOSITORY https://github.com/google/googletest.git GIT_TAG "${DAGFLOW_GOOGLETEST_TAG}"
    OPTIONS "INSTALL_GTEST=OFF" "gtest_force_shared_crt=ON")
endfunction()
