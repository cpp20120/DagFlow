cmake_minimum_required(VERSION 3.26)
include("${CMAKE_CURRENT_LIST_DIR}/../dependencies/VcpkgDiscovery.cmake")
include("${CMAKE_CURRENT_LIST_DIR}/../packaging/LinuxFamily.cmake")
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
if(NOT DEFINED PACKAGE_ARTIFACTS)
  set(PACKAGE_ARTIFACTS OFF)
endif()
if(PACKAGE_FORMAT AND NOT PACKAGE_FORMAT MATCHES "^(TGZ|ZIP|DEB|RPM|NSIS|DragNDrop|ARCH)$")
  message(FATAL_ERROR "Unsupported PACKAGE_FORMAT: ${PACKAGE_FORMAT}")
endif()
if(PACKAGE_FORMAT AND NOT PACKAGE_ARTIFACTS)
  message(FATAL_ERROR "PACKAGE_FORMAT requires PACKAGE_ARTIFACTS=ON")
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
  if(PACKAGE_ARTIFACTS)
    # ARCH is a makepkg backend, not a CPack generator. Do not hand ARCH to CPack.
    set(_cpack_format "${PACKAGE_FORMAT}")
    if(_cpack_format STREQUAL "ARCH")
      set(_cpack_format TGZ)
    endif()
    # Clear any generator choice cached by a previous --package-format invocation.
    # A bare --package must always restore automatic native-format discovery.
    list(APPEND _configure -DENABLE_PACKAGING=ON
      "-DBOILERPLATE_PACKAGE_GENERATORS=${_cpack_format}")
  endif()
  if(BOILERPLATE_VCPKG_BOOTSTRAP)
    list(APPEND _configure -DBOILERPLATE_VCPKG_BOOTSTRAP=ON)
  endif()
  if(_preset MATCHES "vcpkg")
    boilerplate_find_vcpkg(_vcpkg_root "${SOURCE_DIR}")
    message(STATUS "Matrix: vcpkg root=${_vcpkg_root}")
    _validate_vcpkg_registry_checkout("${_vcpkg_root}")
    set(_configure "${CMAKE_COMMAND}" -E env "VCPKG_ROOT=${_vcpkg_root}" ${_configure})
  endif()

  run(${_configure})
  run("${CMAKE_COMMAND}" --build --preset "${_preset}" ${_parallel})
  if(RUN_TESTS)
    run("${CMAKE_CTEST_COMMAND}" --preset "${_preset}" --output-on-failure --no-tests=error)
  endif()
  if(TRAIN_PGO AND _preset MATCHES "pgo-generate$")
    run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target boilerplate_pgo_merge ${_parallel})
  endif()
  if(INSTALL_ARTIFACTS)
    run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target install ${_parallel})
  endif()
  if(PACKAGE_ARTIFACTS)
    # This per-preset directory is reserved for generated distributions.
    # Remove stale packages from earlier runs (e.g. switching from DEB to TGZ),
    # then remove CPack's intermediate install staging after a successful build.
    set(_package_directory "${SOURCE_DIR}/out/packages/${_preset}")
    file(REMOVE_RECURSE "${_package_directory}")
    # Never execute makepkg for explicit non-Arch package formats.
    set(_arch FALSE)
    if(PACKAGE_FORMAT STREQUAL "ARCH")
      set(_arch TRUE)
    elseif(PACKAGE_FORMAT STREQUAL "" AND CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
      boilerplate_linux_family(_family)
      if(_family STREQUAL "arch")
        set(_arch TRUE)
      endif()
    endif()
    if(NOT PACKAGE_FORMAT STREQUAL "ARCH")
      # CPackRPM refuses whitespace in CPACK_TOPLEVEL_DIRECTORY, even though
      # configure, build and install support paths with spaces. When RPM is
      # selected, stage its CPack output at a safe /tmp path and copy the
      # completed RPM back to the standard distributables directory.
      set(_rpm_selected FALSE)
      if(PACKAGE_FORMAT STREQUAL "RPM")
        set(_rpm_selected TRUE)
      elseif(NOT PACKAGE_FORMAT AND CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
        boilerplate_linux_family(_family)
        if(_family STREQUAL "rpm")
          # Without rpmbuild, automatic packaging intentionally falls back to
          # TGZ, just as Packaging.cmake does at configure time.
          find_program(_rpm_tool rpmbuild)
          if(_rpm_tool)
            set(_rpm_selected TRUE)
          endif()
        endif()
      endif()
      if(_rpm_selected AND _package_directory MATCHES "[ \t]")
        set(_cpack_config "${SOURCE_DIR}/out/build/${_preset}/CPackConfig.cmake")
        if(NOT EXISTS "${_cpack_config}")
          message(FATAL_ERROR "Missing CPack configuration: ${_cpack_config}")
        endif()
        file(MAKE_DIRECTORY "${_package_directory}")
        # The default RPM-family generator set is TGZ;RPM. Preserve the TGZ
        # in the regular output folder; explicit --package-format RPM omits it.
        if(NOT PACKAGE_FORMAT)
          run("${CMAKE_CPACK_COMMAND}" --config "${_cpack_config}"
            -G TGZ -B "${_package_directory}")
        endif()
        string(RANDOM LENGTH 16 ALPHABET "0123456789abcdef" _rpm_nonce)
        set(_rpm_staging "/tmp/boilerplate-cpack-rpm-${_rpm_nonce}")
        file(MAKE_DIRECTORY "${_rpm_staging}")
        execute_process(
          COMMAND "${CMAKE_CPACK_COMMAND}" --config "${_cpack_config}"
            -G RPM -B "${_rpm_staging}"
          WORKING_DIRECTORY "${SOURCE_DIR}/out/build/${_preset}"
          RESULT_VARIABLE _rpm_rc)
        if(NOT "${_rpm_rc}" STREQUAL "0")
          file(REMOVE_RECURSE "${_rpm_staging}")
          message(FATAL_ERROR "CPack RPM generation failed (${_rpm_rc})")
        endif()
        file(GLOB _rpm_files "${_rpm_staging}/*.rpm")
        if(NOT _rpm_files)
          file(REMOVE_RECURSE "${_rpm_staging}")
          message(FATAL_ERROR "CPack reported success but generated no RPM")
        endif()
        file(COPY ${_rpm_files} DESTINATION "${_package_directory}")
        file(REMOVE_RECURSE "${_rpm_staging}")
      else()
        # CPack uses install() staging, never installs into the host prefix.
        run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target package ${_parallel})
      endif()
      file(REMOVE_RECURSE "${_package_directory}/_CPack_Packages")
    endif()
    if(_arch)
      run("${CMAKE_COMMAND}" "-DBUILD_DIR=${SOURCE_DIR}/out/build/${_preset}"
        "-DPACKAGE_DIR=${_package_directory}"
        -P "${CMAKE_CURRENT_LIST_DIR}/../packaging/PackageArch.cmake")
    endif()
    message(STATUS "Matrix: distributables at ${_package_directory}")
  endif()
  if(RUN_APPLICATION)
    # Target owns the executable path (including .exe, multi-config and bundles).
    # Never guess a binary location in a shell script.
    run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target boilerplate_run ${_parallel})
  endif()
  if(RUN_TARGET)
    if(NOT RUN_TARGET MATCHES "^[a-zA-Z0-9_.+-]+$")
      message(FATAL_ERROR "Invalid RUN_TARGET: ${RUN_TARGET}")
    endif()
    run("${CMAKE_COMMAND}" --build --preset "${_preset}" --target "${RUN_TARGET}" ${_parallel})
  endif()
endforeach()
