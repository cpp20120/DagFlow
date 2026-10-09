include_guard(GLOBAL)
include(GNUInstallDirs)
include(GenerateExportHeader)
include(CMakePackageConfigHelpers)

set(_shared_default ON)
if(DEFINED BUILD_SHARED_LIBS)
  set(_shared_default "${BUILD_SHARED_LIBS}")
endif()
option(BOILERPLATE_BUILD_SHARED "Build shared library variants" ${_shared_default})
if(BOILERPLATE_BUILD_SHARED)
  set(_static_default OFF)
else()
  set(_static_default ON)
endif()
option(BOILERPLATE_BUILD_STATIC "Build static library variants" ${_static_default})
option(BOILERPLATE_INSTALL "Generate library installation and package exports" ON)
option(BOILERPLATE_BUILD_TESTS "Build dependency-free library smoke tests" OFF)
option(BOILERPLATE_BUILD_BENCHMARKS "Build sample benchmarks (does not run them)" OFF)

# Creates real <name>_shared / <name>_static targets and <name>::<name>,
# which prefers shared if both variants are enabled. All include paths are scoped.
function(boilerplate_add_library name)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "VERSION;INCLUDE_DIR;PACKAGE_CONFIG"
    "SOURCES;PUBLIC_LIBRARIES;PRIVATE_LIBRARIES;POLICIES;POLICY_OPTIONS;SHARED_POLICIES;STATIC_POLICIES;SHARED_POLICY_OPTIONS;STATIC_POLICY_OPTIONS")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES OR NOT ARG_SOURCES OR NOT ARG_INCLUDE_DIR)
    message(FATAL_ERROR "boilerplate_add_library(${name}) requires SOURCES and INCLUDE_DIR; unknown/missing arguments: ${ARG_UNPARSED_ARGUMENTS};${ARG_KEYWORDS_MISSING_VALUES}")
  endif()
  if(NOT ARG_VERSION)
    set(ARG_VERSION "${PROJECT_VERSION}")
  endif()
  if(NOT ARG_VERSION)
    set(ARG_VERSION 1.0.0)
  endif()
  _boilerplate_validate_link_dependencies(${name}
    ${ARG_PUBLIC_LIBRARIES} ${ARG_PRIVATE_LIBRARIES})
  string(MAKE_C_IDENTIFIER "${name}" _prefix)
  string(TOUPPER "${_prefix}" _prefix)
  option(${_prefix}_BUILD_SHARED "Build ${name} shared" ${BOILERPLATE_BUILD_SHARED})
  option(${_prefix}_BUILD_STATIC "Build ${name} static" ${BOILERPLATE_BUILD_STATIC})
  if(NOT ${_prefix}_BUILD_SHARED AND NOT ${_prefix}_BUILD_STATIC)
    message(FATAL_ERROR "Enable ${_prefix}_BUILD_SHARED or ${_prefix}_BUILD_STATIC")
  endif()
  get_filename_component(_include "${ARG_INCLUDE_DIR}" ABSOLUTE)
  set(_generated "${CMAKE_CURRENT_BINARY_DIR}/generated/${name}")
  set(_targets)
  string(REGEX MATCH "^[0-9]+" _major "${ARG_VERSION}")
  foreach(_kind IN ITEMS shared static)
    string(TOUPPER "${_kind}" _type)
    if(NOT ${_prefix}_BUILD_${_type})
      continue()
    endif()
    set(_target "${name}_${_kind}")
    add_library(${_target} ${_type} ${ARG_SOURCES})
    boilerplate_set_artifact_role(${_target} LIBRARY)
    add_library(${name}::${_kind} ALIAS ${_target})
    list(APPEND _targets ${_target})
    target_include_directories(${_target} PUBLIC
      "$<BUILD_INTERFACE:${_include}>"
      "$<BUILD_INTERFACE:${_generated}>"
      "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/${name}>")
    target_link_libraries(${_target} PUBLIC ${ARG_PUBLIC_LIBRARIES} PRIVATE ${ARG_PRIVATE_LIBRARIES})
    set_target_properties(${_target} PROPERTIES
      VERSION "${ARG_VERSION}" SOVERSION "${_major}" EXPORT_NAME "${_kind}"
      CXX_EXTENSIONS OFF POSITION_INDEPENDENT_CODE ON
      CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN ON
      DEFINE_SYMBOL "${_prefix}_EXPORTS")
    set(_target_policies ${ARG_POLICIES} ${ARG_${_type}_POLICIES})
    set(_target_policy_options ${ARG_POLICY_OPTIONS} ${ARG_${_type}_POLICY_OPTIONS})
    set(_policy_args)
    if(_target_policies)
      list(APPEND _policy_args POLICIES ${_target_policies})
    endif()
    if(_target_policy_options)
      list(APPEND _policy_args ${_target_policy_options})
    endif()
    boilerplate_apply_optimization(${_target} ${_policy_args})
    boilerplate_set_output_name(${_target} "${name}_${_kind}")
    if(_kind STREQUAL "static")
      target_compile_definitions(${_target} PUBLIC ${_prefix}_STATIC_DEFINE)
    endif()
  endforeach()
  list(GET _targets 0 _preferred)
  file(MAKE_DIRECTORY "${_generated}")
  generate_export_header(${_preferred} BASE_NAME "${_prefix}"
    EXPORT_FILE_NAME "${_generated}/${name}_export.h"
    EXPORT_MACRO_NAME "${_prefix}_EXPORT"
    STATIC_DEFINE "${_prefix}_STATIC_DEFINE")
  add_library(${name} ALIAS ${_preferred})
  add_library(${name}::${name} ALIAS ${_preferred})

  if(BOILERPLATE_INSTALL)
    set(_package_dir "${CMAKE_INSTALL_LIBDIR}/cmake/${name}")
    install(TARGETS ${_targets} EXPORT ${name}Targets
      ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
      LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime NAMELINK_COMPONENT Development
      RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime)
    install(DIRECTORY "${_include}/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${name}" COMPONENT Development)
    install(FILES "${_generated}/${name}_export.h" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${name}" COMPONENT Development)
    install(EXPORT ${name}Targets NAMESPACE "${name}::" DESTINATION "${_package_dir}" COMPONENT Development)
    set(BOILERPLATE_PACKAGE_NAME "${name}")
    get_target_property(BOILERPLATE_PACKAGE_PREFERRED ${_preferred} EXPORT_NAME)
    if(NOT ARG_PACKAGE_CONFIG)
      set(ARG_PACKAGE_CONFIG "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/LibraryConfig.cmake.in")
    endif()
    configure_package_config_file("${ARG_PACKAGE_CONFIG}"
      "${CMAKE_CURRENT_BINARY_DIR}/${name}Config.cmake" INSTALL_DESTINATION "${_package_dir}")
    write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/${name}ConfigVersion.cmake"
      VERSION "${ARG_VERSION}" COMPATIBILITY SameMajorVersion)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${name}Config.cmake"
      "${CMAKE_CURRENT_BINARY_DIR}/${name}ConfigVersion.cmake" DESTINATION "${_package_dir}" COMPONENT Development)
  endif()
endfunction()
