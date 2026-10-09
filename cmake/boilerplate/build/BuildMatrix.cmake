cmake_minimum_required(VERSION 3.26)
include("${CMAKE_CURRENT_LIST_DIR}/../dependencies/VcpkgDiscovery.cmake")
# Small orchestrator: presets remain the sole source of build policy.
if(NOT SOURCE_DIR)
  set(SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
endif()
get_filename_component(SOURCE_DIR "${SOURCE_DIR}" ABSOLUTE)

# build_all.* should actually exercise both the dependency-free path and the
# manifest/package-manager path. Explicit PRESETS still overrides the profile.
if(NOT MATRIX_PROFILE)
  set(MATRIX_PROFILE all)
endif()
if(NOT MATRIX_PROFILE MATCHES "^(core|vcpkg|all)$")
  message(FATAL_ERROR "MATRIX_PROFILE must be core, vcpkg or all")
endif()
if(NOT PRESETS)
  if(MATRIX_PROFILE STREQUAL "core")
    set(PRESETS app-debug app-release)
  elseif(MATRIX_PROFILE STREQUAL "vcpkg")
    set(PRESETS app-vcpkg-all)
  else()
    set(PRESETS app-debug app-release app-vcpkg-all)
  endif()
endif()

if(NOT ARTIFACT_DIR)
  set(ARTIFACT_DIR "${SOURCE_DIR}/out/artifacts")
endif()
if(NOT DEFINED RUN_TESTS)
  set(RUN_TESTS ON)
endif()
if(NOT DEFINED INSTALL_ARTIFACTS)
  set(INSTALL_ARTIFACTS ON)
endif()
set(_parallel)
if(JOBS)
  if(NOT JOBS MATCHES "^[1-9][0-9]*$")
    message(FATAL_ERROR "JOBS must be a positive integer")
  endif()
  set(_parallel --parallel "${JOBS}")
endif()

function(_validate_vcpkg_registry_checkout root)
  set(_manifest "${SOURCE_DIR}/vcpkg.json")
  if(NOT EXISTS "${_manifest}")
    return()
  endif()

  file(READ "${_manifest}" _manifest_json)
  string(JSON _baseline ERROR_VARIABLE _baseline_error GET "${_manifest_json}" builtin-baseline)
  if(_baseline_error OR NOT _baseline)
    return()
  endif()

  find_program(_git git)
  if(NOT _git)
    message(FATAL_ERROR
      "vcpkg manifest pins builtin-baseline ${_baseline}, but git is not available "
      "to validate the VCPKG_ROOT checkout")
  endif()

  execute_process(
    COMMAND "${_git}" -C "${root}" cat-file -e "${_baseline}:versions/baseline.json"
    RESULT_VARIABLE _baseline_result
    OUTPUT_QUIET
    ERROR_QUIET)
  if("${_baseline_result}" STREQUAL "0")
    return()
  endif()

  execute_process(
    COMMAND "${_git}" -C "${root}" rev-parse --is-shallow-repository
    RESULT_VARIABLE _shallow_result
    OUTPUT_VARIABLE _is_shallow
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)

  if("${_shallow_result}" STREQUAL "0" AND _is_shallow STREQUAL "true")
    set(_repair "git -C ${root} fetch origin --unshallow --tags")
  else()
    set(_repair "git -C ${root} fetch origin --tags --prune")
  endif()

  message(FATAL_ERROR
    "VCPKG_ROOT cannot resolve builtin-baseline ${_baseline} at "
    "versions/baseline.json. The checkout is stale, shallow, or incomplete.\n"
    "Repair it, then rerun the matrix:\n  ${_repair}\n"
    "Verify with:\n  git -C ${root} show ${_baseline}:versions/baseline.json > /dev/null")
endfunction()

function(run)
  execute_process(COMMAND ${ARGV} WORKING_DIRECTORY "${SOURCE_DIR}" RESULT_VARIABLE _result)
  if(NOT "${_result}" STREQUAL "0")
    message(FATAL_ERROR "Matrix step failed (${_result}): ${ARGV}")
  endif()
endfunction()

foreach(_preset IN LISTS PRESETS)
  if(NOT _preset MATCHES "^[a-zA-Z0-9_-]+$")
    message(FATAL_ERROR "Invalid matrix preset name: ${_preset}")
  endif()
  message(STATUS "Matrix: ${_preset}")

  set(_configure "${CMAKE_COMMAND}" --preset "${_preset}"
    "-DCMAKE_INSTALL_PREFIX=${ARTIFACT_DIR}/${_preset}" ${CONFIGURE_ARGS})
  if(_preset MATCHES "vcpkg")
    boilerplate_find_vcpkg(_vcpkg_root "${SOURCE_DIR}")
    message(STATUS "Matrix: vcpkg root=${_vcpkg_root}")
    _validate_vcpkg_registry_checkout("${_vcpkg_root}")
    set(_configure "${CMAKE_COMMAND}" -E env "VCPKG_ROOT=${_vcpkg_root}" ${_configure})
  endif()

  run(${_configure})
  run("${CMAKE_COMMAND}" --build --preset "${_preset}" ${_parallel})
  if(RUN_TESTS)
    run("${CMAKE_CTEST_COMMAND}" --preset "${_preset}")
  endif()
  if(TRAIN_PGO AND _preset MATCHES "pgo-generate$")
    run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target boilerplate_pgo_merge ${_parallel})
  endif()
  if(INSTALL_ARTIFACTS)
    run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target install ${_parallel})
  endif()
endforeach()
