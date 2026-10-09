include_guard(GLOBAL)

# Components are a workspace-scale grouping layer above targets. They deliberately
# do not own a second project lifecycle: boilerplate_project()/finalize_project()
# still run once per CMake configure. A component only supplies identity, source
# ownership and IDE grouping for a subtree of targets.
define_property(DIRECTORY PROPERTY BOILERPLATE_COMPONENT INHERITED
        BRIEF_DOCS "Boilerplate component owning targets created in this directory subtree"
        FULL_DOCS "Inherited component identity used by boilerplate_register_target().")

define_property(TARGET PROPERTY BOILERPLATE_COMPONENT
        BRIEF_DOCS "Boilerplate component that owns this target"
        FULL_DOCS "Workspace component identity recorded when a target is registered.")

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

function(_boilerplate_component_id name output)
  string(MAKE_C_IDENTIFIER "${name}" _id)
  string(TOUPPER "${_id}" _id)
  set(${output} "${_id}" PARENT_SCOPE)
endfunction()

function(_boilerplate_normalize_component_path input base output)
  if(IS_ABSOLUTE "${input}")
    get_filename_component(_path "${input}" ABSOLUTE)
  else()
    get_filename_component(_path "${input}" ABSOLUTE BASE_DIR "${base}")
  endif()
  if(EXISTS "${_path}")
    get_filename_component(_path "${_path}" REALPATH)
  endif()
  set(${output} "${_path}" PARENT_SCOPE)
endfunction()

function(_boilerplate_register_component_metadata name)
  get_property(_frozen GLOBAL PROPERTY BOILERPLATE_PROJECT_GRAPH_FROZEN)
  if(_frozen)
    message(FATAL_ERROR "Cannot register component '${name}' after boilerplate_finalize_project() begins")
  endif()
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "SOURCE_DIR;BINARY_DIR;VERSION;FOLDER;DESCRIPTION" "")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES)
    message(FATAL_ERROR "boilerplate component '${name}': invalid arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()
  if(NOT name MATCHES "^[A-Za-z][A-Za-z0-9_.+-]*$")
    message(FATAL_ERROR "Invalid boilerplate component name '${name}'")
  endif()

  if(NOT ARG_SOURCE_DIR)
    set(ARG_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()
  if(NOT ARG_BINARY_DIR)
    set(ARG_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  endif()
  _boilerplate_normalize_component_path("${ARG_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" _source)
  _boilerplate_normalize_component_path("${ARG_BINARY_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" _binary)

  _boilerplate_component_id("${name}" _id)
  get_property(_defined GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_DEFINED")
  if(_defined)
    get_property(_old_name GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_NAME")
    get_property(_old_source GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_SOURCE_DIR")
    get_property(_old_binary GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_BINARY_DIR")
    if(NOT _old_name STREQUAL name)
      message(FATAL_ERROR "Component names '${_old_name}' and '${name}' map to the same identifier ${_id}")
    endif()
    if(NOT _old_source STREQUAL _source OR NOT _old_binary STREQUAL _binary)
      message(FATAL_ERROR
              "Component '${name}' is already registered at ${_old_source} -> ${_old_binary}; "
              "cannot also register ${_source} -> ${_binary}")
    endif()
  else()
    set_property(GLOBAL APPEND PROPERTY BOILERPLATE_COMPONENTS "${name}")
    set_property(GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_DEFINED" TRUE)
    set_property(GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_NAME" "${name}")
    set_property(GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_SOURCE_DIR" "${_source}")
    set_property(GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_BINARY_DIR" "${_binary}")
  endif()

  foreach(_field IN ITEMS VERSION FOLDER DESCRIPTION)
    if(ARG_${_field})
      get_property(_old GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_${_field}")
      if(_old AND NOT _old STREQUAL "${ARG_${_field}}")
        message(FATAL_ERROR
                "Component '${name}' ${_field} is already '${_old}', cannot change to '${ARG_${_field}}'")
      endif()
      set_property(GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_${_field}" "${ARG_${_field}}")
    endif()
  endforeach()
endfunction()

# Register the current source/binary directory as a component. This is useful in
# a subtree that can also be configured standalone: the same CMakeLists can call
# boilerplate_component() in both standalone and embedded modes without creating
# another project lifecycle.
function(boilerplate_component name)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "VERSION;FOLDER;DESCRIPTION" "")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES)
    message(FATAL_ERROR "boilerplate_component(${name}): invalid arguments")
  endif()
  set(_metadata_args
          SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
          BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  foreach(_field IN ITEMS VERSION FOLDER DESCRIPTION)
    if(DEFINED ARG_${_field} AND NOT "${ARG_${_field}}" STREQUAL "")
      list(APPEND _metadata_args ${_field} "${ARG_${_field}}")
    endif()
  endforeach()
  _boilerplate_register_component_metadata("${name}" ${_metadata_args})
  set_property(DIRECTORY PROPERTY BOILERPLATE_COMPONENT "${name}")
endfunction()

# Workspace convenience: register a component and configure its subtree. The
# component identity is temporarily inherited through the child directory, then
# the parent directory scope is restored. Small projects can keep using ordinary
# add_subdirectory() and never opt into this layer.
function(_boilerplate_prepare_component_add name output_source output_binary output_exclude output_system)
  # Parse in a function so PARSE_ARGV preserves literal semicolons in values,
  # while the public boilerplate_add_component() remains a macro. The macro is
  # intentional: wrapping add_subdirectory() in a function inserts an extra
  # variable scope and breaks the normal child -> parent PARENT_SCOPE contract.
  cmake_parse_arguments(PARSE_ARGV 5 ARG "EXCLUDE_FROM_ALL;SYSTEM"
          "SOURCE_DIR;BINARY_DIR;VERSION;FOLDER;DESCRIPTION" "")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES)
    message(FATAL_ERROR "boilerplate_add_component(${name}): invalid arguments")
  endif()
  if(NOT ARG_SOURCE_DIR)
    set(ARG_SOURCE_DIR "${name}")
  endif()
  _boilerplate_normalize_component_path("${ARG_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" _source)
  if(_source STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    message(FATAL_ERROR
            "boilerplate_add_component(${name}) cannot add the current source directory; "
            "use boilerplate_component(${name}) to register it")
  endif()
  if(NOT EXISTS "${_source}/CMakeLists.txt")
    message(FATAL_ERROR "Component '${name}' has no CMakeLists.txt: ${_source}")
  endif()

  if(ARG_BINARY_DIR)
    _boilerplate_normalize_component_path("${ARG_BINARY_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" _binary)
  else()
    # Mirror add_subdirectory(source) semantics for an in-tree source and keep
    # external/absolute component trees collision-free by component name.
    file(RELATIVE_PATH _relative "${CMAKE_CURRENT_SOURCE_DIR}" "${_source}")
    if(NOT _relative MATCHES "^\\.\\./" AND NOT IS_ABSOLUTE "${_relative}")
      get_filename_component(_binary "${CMAKE_CURRENT_BINARY_DIR}/${_relative}" ABSOLUTE)
    else()
      get_filename_component(_binary "${CMAKE_CURRENT_BINARY_DIR}/components/${name}" ABSOLUTE)
    endif()
  endif()

  set(_metadata_args SOURCE_DIR "${_source}" BINARY_DIR "${_binary}")
  foreach(_field IN ITEMS VERSION FOLDER DESCRIPTION)
    if(DEFINED ARG_${_field} AND NOT "${ARG_${_field}}" STREQUAL "")
      list(APPEND _metadata_args ${_field} "${ARG_${_field}}")
    endif()
  endforeach()
  _boilerplate_register_component_metadata("${name}" ${_metadata_args})
  _boilerplate_component_id("${name}" _id)
  get_property(_already_added GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_ADDED")
  if(_already_added)
    message(FATAL_ERROR "Component '${name}' was already added to this workspace")
  endif()

  set(${output_source} "${_source}" PARENT_SCOPE)
  set(${output_binary} "${_binary}" PARENT_SCOPE)
  set(${output_exclude} "${ARG_EXCLUDE_FROM_ALL}" PARENT_SCOPE)
  set(${output_system} "${ARG_SYSTEM}" PARENT_SCOPE)
endfunction()

# This is deliberately a macro rather than a function. add_subdirectory() has
# observable variable-scope semantics: a child using set(... PARENT_SCOPE)
# expects to publish into the directory that added it. A function wrapper would
# swallow those values in its temporary function scope and would therefore not
# be a transparent scaling layer for existing CMake subprojects.
macro(boilerplate_add_component name)
  _boilerplate_prepare_component_add("${name}"
          _boilerplate_component_source
          _boilerplate_component_binary
          _boilerplate_component_exclude
          _boilerplate_component_system
          ${ARGN})

  get_property(_boilerplate_component_old DIRECTORY PROPERTY BOILERPLATE_COMPONENT)
  set_property(DIRECTORY PROPERTY BOILERPLATE_COMPONENT "${name}")

  set(_boilerplate_component_add_args
          "${_boilerplate_component_source}" "${_boilerplate_component_binary}")
  if(_boilerplate_component_exclude)
    list(APPEND _boilerplate_component_add_args EXCLUDE_FROM_ALL)
  endif()
  if(_boilerplate_component_system)
    list(APPEND _boilerplate_component_add_args SYSTEM)
  endif()
  add_subdirectory(${_boilerplate_component_add_args})

  _boilerplate_component_id("${name}" _boilerplate_component_registry_id)
  set_property(GLOBAL PROPERTY
          "BOILERPLATE_COMPONENT_${_boilerplate_component_registry_id}_ADDED" TRUE)
  set_property(DIRECTORY PROPERTY BOILERPLATE_COMPONENT "${_boilerplate_component_old}")

  unset(_boilerplate_component_source)
  unset(_boilerplate_component_binary)
  unset(_boilerplate_component_exclude)
  unset(_boilerplate_component_system)
  unset(_boilerplate_component_old)
  unset(_boilerplate_component_add_args)
  unset(_boilerplate_component_registry_id)
endmacro()

# Register a project-owned target with the workspace model. Framework target
# constructors call this automatically through boilerplate_set_artifact_role().
# Raw add_library()/add_executable() remains an escape hatch; call this function
# explicitly when such a target should participate in workspace-wide workflows.
function(boilerplate_register_target target)
  get_property(_frozen GLOBAL PROPERTY BOILERPLATE_PROJECT_GRAPH_FROZEN)
  if(_frozen)
    message(FATAL_ERROR "Cannot register target '${target}' after boilerplate_finalize_project() begins")
  endif()
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "COMPONENT" "")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES)
    message(FATAL_ERROR "boilerplate_register_target(${target}): invalid arguments")
  endif()
  if(NOT TARGET ${target})
    message(FATAL_ERROR "boilerplate_register_target: unknown target ${target}")
  endif()

  get_target_property(_real ${target} ALIASED_TARGET)
  if(NOT _real)
    set(_real "${target}")
  endif()
  get_target_property(_imported ${_real} IMPORTED)
  if(_imported)
    return()
  endif()

  if(ARG_COMPONENT)
    set(_component "${ARG_COMPONENT}")
  else()
    get_property(_component DIRECTORY PROPERTY BOILERPLATE_COMPONENT)
  endif()

  get_target_property(_old_component ${_real} BOILERPLATE_COMPONENT)
  if(_old_component MATCHES "-NOTFOUND$")
    set(_old_component "")
  endif()
  if(_old_component AND _component AND NOT _old_component STREQUAL _component)
    message(FATAL_ERROR
            "Target ${_real} is already owned by component '${_old_component}', cannot move to '${_component}'")
  endif()
  if(NOT _component)
    set(_component "${_old_component}")
  endif()

  get_property(_targets GLOBAL PROPERTY BOILERPLATE_PROJECT_TARGETS)
  if(NOT _real IN_LIST _targets)
    set_property(GLOBAL APPEND PROPERTY BOILERPLATE_PROJECT_TARGETS "${_real}")
  endif()

  if(_component)
    _boilerplate_component_id("${_component}" _id)
    get_property(_defined GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_DEFINED")
    if(NOT _defined)
      message(FATAL_ERROR
              "Target ${_real} refers to unknown component '${_component}'; call boilerplate_component() "
              "or boilerplate_add_component() first")
    endif()
    set_property(TARGET ${_real} PROPERTY BOILERPLATE_COMPONENT "${_component}")
    get_property(_component_targets GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_TARGETS")
    if(NOT _real IN_LIST _component_targets)
      set_property(GLOBAL APPEND PROPERTY "BOILERPLATE_COMPONENT_${_id}_TARGETS" "${_real}")
    endif()

    get_property(_folder GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_FOLDER")
    if(_folder)
      get_target_property(_target_folder ${_real} FOLDER)
      if(NOT _target_folder OR _target_folder MATCHES "-NOTFOUND$")
        set_property(TARGET ${_real} PROPERTY FOLDER "${_folder}")
      endif()
    endif()
  endif()
endfunction()

function(boilerplate_list_components output)
  get_property(_components GLOBAL PROPERTY BOILERPLATE_COMPONENTS)
  set(${output} "${_components}" PARENT_SCOPE)
endfunction()

function(boilerplate_component_targets name output)
  _boilerplate_component_id("${name}" _id)
  get_property(_defined GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_DEFINED")
  if(NOT _defined)
    message(FATAL_ERROR "Unknown boilerplate component '${name}'")
  endif()
  get_property(_targets GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_TARGETS")
  set(${output} "${_targets}" PARENT_SCOPE)
endfunction()

function(boilerplate_component_property name property output)
  _boilerplate_component_id("${name}" _id)
  get_property(_defined GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_DEFINED")
  if(NOT _defined)
    message(FATAL_ERROR "Unknown boilerplate component '${name}'")
  endif()
  string(TOUPPER "${property}" _property)
  if(NOT _property MATCHES "^(SOURCE_DIR|BINARY_DIR|VERSION|FOLDER|DESCRIPTION)$")
    message(FATAL_ERROR "Unsupported component property '${property}'")
  endif()
  get_property(_value GLOBAL PROPERTY "BOILERPLATE_COMPONENT_${_id}_${_property}")
  set(${output} "${_value}" PARENT_SCOPE)
endfunction()
