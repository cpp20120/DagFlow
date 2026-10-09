include_guard(GLOBAL)
option(BOILERPLATE_FETCH_DEPENDENCIES "Allow explicit pinned FetchContent fallback if packages are absent" OFF)
set(BOILERPLATE_CPM_FILE "" CACHE FILEPATH "Optional CPM.cmake file used when BOILERPLATE_DEPENDENCY_PROVIDER=cpm")

# Resolve a package without making a dependency manager part of target/domain APIs.
# system/vcpkg consume installed CMake packages; fetchcontent/cpm first try the
# installed package and then use the explicit pinned repository supplied by the caller.
function(boilerplate_require_dependency)
  cmake_parse_arguments(PARSE_ARGV 0 ARG "NO_SUBMODULES" "NAME;PACKAGE;TARGET;VERSION;GIT_REPOSITORY;GIT_TAG" "COMPONENTS;OPTIONS")
  if(ARG_UNPARSED_ARGUMENTS OR NOT ARG_NAME OR NOT ARG_PACKAGE OR NOT ARG_TARGET)
    message(FATAL_ERROR "boilerplate_require_dependency requires NAME, PACKAGE and TARGET")
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

  if(BOILERPLATE_DEPENDENCY_PROVIDER MATCHES "^(none|system|vcpkg)$")
    message(FATAL_ERROR
      "Dependency ${ARG_PACKAGE} did not provide ${ARG_TARGET}. Install it for provider "
      "'${BOILERPLATE_DEPENDENCY_PROVIDER}' or select an explicit fetch provider.")
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

  if(BOILERPLATE_DEPENDENCY_PROVIDER STREQUAL "fetchcontent")
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
  elseif(BOILERPLATE_DEPENDENCY_PROVIDER STREQUAL "cpm")
    if(NOT COMMAND CPMAddPackage)
      if(BOILERPLATE_CPM_FILE AND EXISTS "${BOILERPLATE_CPM_FILE}")
        include("${BOILERPLATE_CPM_FILE}")
      else()
        message(FATAL_ERROR "CPM provider requires CPMAddPackage or BOILERPLATE_CPM_FILE")
      endif()
    endif()
    CPMAddPackage(NAME ${ARG_NAME}
      GIT_REPOSITORY "${ARG_GIT_REPOSITORY}"
      GIT_TAG "${ARG_GIT_TAG}"
      OPTIONS ${ARG_OPTIONS})
  else()
    message(FATAL_ERROR "Unsupported dependency provider: ${BOILERPLATE_DEPENDENCY_PROVIDER}")
  endif()

  if(NOT TARGET ${ARG_TARGET})
    message(FATAL_ERROR "Dependency ${ARG_NAME} resolved but expected target ${ARG_TARGET} is missing")
  endif()
endfunction()


set(BOILERPLATE_RAPIDCHECK_TAG ff6af6fc683159deb51c543b065eba14dfcf329b CACHE STRING
  "Pinned RapidCheck revision used by explicit fetch fallback")
set(BOILERPLATE_FUZZTEST_TAG 2026-06-29 CACHE STRING
  "Pinned Google FuzzTest release used by explicit fetch fallback")
set(BOILERPLATE_RAPIDCHECK_SOURCE_DIR "" CACHE PATH
  "Existing RapidCheck source tree; preferred over downloading it")
set(BOILERPLATE_FUZZTEST_SOURCE_DIR "" CACHE PATH
  "Existing Google FuzzTest source tree; preferred over downloading it")
set(BOILERPLATE_FUZZTEST_MODE "unit" CACHE STRING
  "Google FuzzTest mode: unit, fuzzing, libfuzzer")
set_property(CACHE BOILERPLATE_FUZZTEST_MODE PROPERTY STRINGS unit fuzzing libfuzzer)

function(boilerplate_require_rapidcheck)
  if(TARGET rapidcheck AND TARGET rapidcheck_gtest)
    return()
  endif()
  # vcpkg installs RapidCheck's extras (including rapidcheck_gtest). For an
  # explicit source fallback request the same modules before adding the project.
  set(RC_ENABLE_GTEST ON CACHE BOOL "RapidCheck GoogleTest integration" FORCE)
  set(RC_INSTALL_ALL_EXTRAS ON CACHE BOOL "RapidCheck extras" FORCE)
  set(RC_ENABLE_TESTS OFF CACHE BOOL "RapidCheck upstream tests" FORCE)
  if(BOILERPLATE_RAPIDCHECK_SOURCE_DIR)
    if(NOT EXISTS "${BOILERPLATE_RAPIDCHECK_SOURCE_DIR}/CMakeLists.txt")
      message(FATAL_ERROR "BOILERPLATE_RAPIDCHECK_SOURCE_DIR has no CMakeLists.txt: ${BOILERPLATE_RAPIDCHECK_SOURCE_DIR}")
    endif()
    add_subdirectory("${BOILERPLATE_RAPIDCHECK_SOURCE_DIR}"
      "${CMAKE_BINARY_DIR}/_deps/rapidcheck-build" EXCLUDE_FROM_ALL)
  else()
    boilerplate_require_dependency(
      NAME rapidcheck PACKAGE rapidcheck TARGET rapidcheck
      NO_SUBMODULES
      GIT_REPOSITORY https://github.com/emil-e/rapidcheck.git
      GIT_TAG "${BOILERPLATE_RAPIDCHECK_TAG}"
      OPTIONS "RC_ENABLE_GTEST=ON" "RC_INSTALL_ALL_EXTRAS=ON" "RC_ENABLE_TESTS=OFF")
  endif()
  if(NOT TARGET rapidcheck_gtest)
    message(FATAL_ERROR
      "RapidCheck resolved but rapidcheck_gtest is missing. The package must be built with RC_ENABLE_GTEST/RC_INSTALL_ALL_EXTRAS.")
  endif()
endfunction()

function(boilerplate_require_fuzztest)
  cmake_parse_arguments(PARSE_ARGV 0 ARG "" "MODE" "")
  if(NOT ARG_MODE)
    set(ARG_MODE "${BOILERPLATE_FUZZTEST_MODE}")
  endif()
  if(NOT ARG_MODE MATCHES "^(unit|fuzzing|libfuzzer)$")
    message(FATAL_ERROR "Google FuzzTest mode must be unit, fuzzing or libfuzzer")
  endif()

  get_property(_mode GLOBAL PROPERTY BOILERPLATE_FUZZTEST_RESOLVED_MODE)
  if(_mode AND NOT "${_mode}" STREQUAL "${ARG_MODE}")
    message(FATAL_ERROR "One CMake build can only use one Google FuzzTest mode: ${_mode} vs ${ARG_MODE}")
  endif()
  if(COMMAND link_fuzztest)
    set_property(GLOBAL PROPERTY BOILERPLATE_FUZZTEST_RESOLVED_MODE "${ARG_MODE}")
    return()
  endif()

  if(NOT ARG_MODE STREQUAL "unit")
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      message(FATAL_ERROR "Google FuzzTest fuzzing/compatibility mode requires Linux + Clang")
    endif()
  endif()
  set(FUZZTEST_BUILD_TESTING OFF CACHE BOOL "FuzzTest upstream tests" FORCE)
  set(FUZZTEST_FUZZING_MODE OFF CACHE BOOL "FuzzTest coverage-guided mode" FORCE)
  set(FUZZTEST_COMPATIBILITY_MODE "" CACHE STRING "FuzzTest compatibility mode" FORCE)
  if(ARG_MODE STREQUAL "fuzzing")
    set(FUZZTEST_FUZZING_MODE ON CACHE BOOL "FuzzTest coverage-guided mode" FORCE)
  elseif(ARG_MODE STREQUAL "libfuzzer")
    set(FUZZTEST_COMPATIBILITY_MODE libfuzzer CACHE STRING "FuzzTest compatibility mode" FORCE)
  endif()

  if(BOILERPLATE_FUZZTEST_SOURCE_DIR)
    if(NOT EXISTS "${BOILERPLATE_FUZZTEST_SOURCE_DIR}/CMakeLists.txt")
      message(FATAL_ERROR "BOILERPLATE_FUZZTEST_SOURCE_DIR has no CMakeLists.txt: ${BOILERPLATE_FUZZTEST_SOURCE_DIR}")
    endif()
    add_subdirectory("${BOILERPLATE_FUZZTEST_SOURCE_DIR}"
      "${CMAKE_BINARY_DIR}/_deps/fuzztest-build" EXCLUDE_FROM_ALL)
  elseif(BOILERPLATE_FETCH_DEPENDENCIES OR BOILERPLATE_DEPENDENCY_PROVIDER MATCHES "^(fetchcontent|cpm)$")
    include(FetchContent)
    FetchContent_Declare(boilerplate_fuzztest
      GIT_REPOSITORY https://github.com/google/fuzztest.git
      GIT_TAG "${BOILERPLATE_FUZZTEST_TAG}"
      GIT_SHALLOW TRUE)
    FetchContent_MakeAvailable(boilerplate_fuzztest)
  else()
    message(FATAL_ERROR
      "Google FuzzTest has no normal vcpkg/system CMake package path here. Add it before Boilerplate, set BOILERPLATE_FUZZTEST_SOURCE_DIR, or explicitly enable FetchContent.")
  endif()
  if(NOT COMMAND link_fuzztest)
    message(FATAL_ERROR "Google FuzzTest was added but did not define link_fuzztest()")
  endif()
  set_property(GLOBAL PROPERTY BOILERPLATE_FUZZTEST_RESOLVED_MODE "${ARG_MODE}")
endfunction()

set(BOILERPLATE_GOOGLE_BENCHMARK_TAG v1.9.5 CACHE STRING "Pinned Google Benchmark revision")
set(BOILERPLATE_GOOGLETEST_TAG v1.17.0 CACHE STRING "Pinned GoogleTest revision")
set(BOILERPLATE_ALLOCATOR system CACHE STRING "Allocator adapter: system, mimalloc, tbbmalloc")
set_property(CACHE BOILERPLATE_ALLOCATOR PROPERTY STRINGS system mimalloc tbbmalloc)

function(boilerplate_require_google_benchmark)
  if(TARGET benchmark::benchmark_main)
    return()
  endif()
  if(BOILERPLATE_FETCH_DEPENDENCIES AND BOILERPLATE_DEPENDENCY_PROVIDER MATCHES "^(none|system|vcpkg)$")
    set(BOILERPLATE_DEPENDENCY_PROVIDER fetchcontent)
  endif()
  boilerplate_require_dependency(
    NAME googlebenchmark PACKAGE benchmark TARGET benchmark::benchmark_main VERSION 1.8
    GIT_REPOSITORY https://github.com/google/benchmark.git GIT_TAG "${BOILERPLATE_GOOGLE_BENCHMARK_TAG}"
    OPTIONS "BENCHMARK_ENABLE_TESTING=OFF" "BENCHMARK_ENABLE_INSTALL=OFF" "BENCHMARK_ENABLE_WERROR=OFF")
endfunction()

function(boilerplate_require_googletest)
  if(TARGET GTest::gtest_main)
    return()
  endif()
  if(BOILERPLATE_FETCH_DEPENDENCIES AND BOILERPLATE_DEPENDENCY_PROVIDER MATCHES "^(none|system|vcpkg)$")
    set(BOILERPLATE_DEPENDENCY_PROVIDER fetchcontent)
  endif()
  boilerplate_require_dependency(
    NAME googletest PACKAGE GTest TARGET GTest::gtest_main
    GIT_REPOSITORY https://github.com/google/googletest.git GIT_TAG "${BOILERPLATE_GOOGLETEST_TAG}"
    OPTIONS "INSTALL_GTEST=OFF" "gtest_force_shared_crt=ON")
endfunction()

# Explicit allocation APIs: selecting a library does not replace malloc globally.
function(boilerplate_use_allocator target)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "ALLOCATOR;PREFIX" "")
  if(ARG_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "Invalid allocator arguments")
  endif()
  if(NOT ARG_ALLOCATOR)
    set(ARG_ALLOCATOR "${BOILERPLATE_ALLOCATOR}")
  endif()
  if(NOT ARG_PREFIX)
    set(ARG_PREFIX BOILERPLATE)
  endif()
  if(ARG_ALLOCATOR STREQUAL "mimalloc")
    find_package(mimalloc CONFIG REQUIRED)
    target_link_libraries(${target} PRIVATE mimalloc)
    target_compile_definitions(${target} PRIVATE ${ARG_PREFIX}_USE_MIMALLOC=1)
  elseif(ARG_ALLOCATOR STREQUAL "tbbmalloc")
    find_package(TBB CONFIG REQUIRED COMPONENTS tbbmalloc)
    target_link_libraries(${target} PRIVATE TBB::tbbmalloc)
    target_compile_definitions(${target} PRIVATE ${ARG_PREFIX}_USE_TBBMALLOC=1)
  elseif(NOT ARG_ALLOCATOR STREQUAL "system")
    message(FATAL_ERROR "Unknown allocator: ${ARG_ALLOCATOR}")
  endif()
endfunction()

# The reference owns its cache/target namespace. Source is explicit; no downloads.
function(boilerplate_add_reference name)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "TEST;INSTALL" "SOURCE_DIR" "CMAKE_ARGS")
  if(ARG_UNPARSED_ARGUMENTS OR NOT EXISTS "${ARG_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Reference ${name} needs SOURCE_DIR containing CMakeLists.txt")
  endif()
  include(ExternalProject)
  set(_install INSTALL_COMMAND "${CMAKE_COMMAND}" -E true)
  if(ARG_INSTALL)
    set(_install)
  endif()
  set(_test)
  if(ARG_TEST)
    if(CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR)
      message(FATAL_ERROR "Reference ${name}: TEST in a cross build requires CMAKE_CROSSCOMPILING_EMULATOR")
    endif()
    set(_test TEST_AFTER_INSTALL FALSE TEST_BEFORE_INSTALL TRUE)
  endif()
  set(_platform_args)
  if(CMAKE_CROSSCOMPILING)
    foreach(_key IN ITEMS CMAKE_TOOLCHAIN_FILE CMAKE_SYSROOT CMAKE_C_COMPILER CMAKE_CXX_COMPILER
        CMAKE_FIND_ROOT_PATH CMAKE_CROSSCOMPILING_EMULATOR ${CMAKE_TRY_COMPILE_PLATFORM_VARIABLES})
      if(DEFINED ${_key})
        string(REPLACE ";" "|" _value "${${_key}}")
        list(APPEND _platform_args "-D${_key}:STRING=${_value}")
      endif()
    endforeach()
  endif()
  ExternalProject_Add(${name} SOURCE_DIR "${ARG_SOURCE_DIR}"
    BINARY_DIR "${CMAKE_BINARY_DIR}/references/${name}/build"
    INSTALL_DIR "${CMAKE_BINARY_DIR}/references/${name}/install"
    DOWNLOAD_COMMAND "" UPDATE_COMMAND "" EXCLUDE_FROM_ALL TRUE BUILD_ALWAYS TRUE
    LIST_SEPARATOR "|"
    CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>" ${_platform_args} ${ARG_CMAKE_ARGS}
    ${_install} ${_test})
endfunction()

# Compare Google Benchmark JSON directly in CMake. This keeps the framework
# self-contained; no upstream compare.py/Python runtime is required.
function(boilerplate_add_google_comparison name)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "BASELINE;CANDIDATE;MAX_REGRESSION_PERCENT" "")
  if(ARG_UNPARSED_ARGUMENTS OR NOT ARG_BASELINE OR NOT ARG_CANDIDATE)
    message(FATAL_ERROR "Google comparison requires BASELINE and CANDIDATE JSON files")
  endif()
  if(NOT DEFINED ARG_MAX_REGRESSION_PERCENT OR ARG_MAX_REGRESSION_PERCENT STREQUAL "")
    set(ARG_MAX_REGRESSION_PERCENT 0)
  endif()
  add_custom_target(${name}
    COMMAND "${CMAKE_COMMAND}"
      "-DBASELINE=${ARG_BASELINE}"
      "-DCANDIDATE=${ARG_CANDIDATE}"
      "-DMAX_REGRESSION_PERCENT=${ARG_MAX_REGRESSION_PERCENT}"
      -P "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../benchmark/GoogleBenchmarkCompare.cmake"
    USES_TERMINAL VERBATIM)
endfunction()
