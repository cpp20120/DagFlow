include_guard(GLOBAL)

# Target policies are named compositions of build settings. The cache-level
# BOILERPLATE_* variables remain project defaults; a policy can override them
# for one target without creating a preset/profile cross product.
set_property(GLOBAL PROPERTY BOILERPLATE_POLICY_KEYS
  "CXX_STANDARD;WARNINGS;WARNINGS_AS_ERRORS;HARDENING;CFI;CFI_DIAGNOSTICS;WINDOWS_CFG;COVERAGE;REPRODUCIBLE;UNITY_BUILD;CCACHE;CLANG_TIDY;FRAME_POINTERS;DEBUG_SYMBOLS;SANITIZER;LTO_MODE;PGO_MODE;PGO_DIR;PGO_PROFILE;ENABLE_NATIVE;ENABLE_NO_SEMANTIC_INTERPOSITION;ENABLE_GC_SECTIONS;ENABLE_NO_PLT;USE_LLD;ENABLE_ICF;ARTIFACT_SUFFIX")
set_property(GLOBAL APPEND PROPERTY BOILERPLATE_POLICY_KEYS
  CUDA CUDA_STANDARD CUDA_SEPARABLE_COMPILATION CUDA_RUNTIME_LIBRARY CUDA_LINEINFO CUDA_DEVICE_DEBUG CUDA_FAST_MATH)

function(_boilerplate_policy_id name output)
  string(MAKE_C_IDENTIFIER "${name}" _id)
  string(TOUPPER "${_id}" _id)
  set(${output} "${_id}" PARENT_SCOPE)
endfunction()

function(_boilerplate_default_setting key output)
  if(key MATCHES "^(CUDA|CUDA_LINEINFO|CUDA_DEVICE_DEBUG|CUDA_FAST_MATH)$")
    set(_value OFF)
  elseif(key MATCHES "^(CUDA_STANDARD|CUDA_SEPARABLE_COMPILATION|CUDA_RUNTIME_LIBRARY)$")
    set(_value "${BOILERPLATE_${key}}")
  elseif(key STREQUAL "CXX_STANDARD")
    set(_value "${BOILERPLATE_CXX_STANDARD}")
  elseif(key STREQUAL "WARNINGS")
    set(_value "${BOILERPLATE_WARNINGS}")
  elseif(key STREQUAL "WARNINGS_AS_ERRORS")
    set(_value "${BOILERPLATE_WARNINGS_AS_ERRORS}")
  elseif(key STREQUAL "HARDENING")
    set(_value "${BOILERPLATE_HARDENING}")
  elseif(key MATCHES "^(CFI|CFI_DIAGNOSTICS|WINDOWS_CFG)$")
    set(_value "${BOILERPLATE_${key}}")
  elseif(key STREQUAL "COVERAGE")
    set(_value "${BOILERPLATE_COVERAGE}")
  elseif(key STREQUAL "REPRODUCIBLE")
    set(_value "${BOILERPLATE_REPRODUCIBLE}")
  elseif(key STREQUAL "UNITY_BUILD")
    set(_value "${BOILERPLATE_UNITY_BUILD}")
  elseif(key STREQUAL "CCACHE")
    set(_value "${BOILERPLATE_CCACHE}")
  elseif(key STREQUAL "CLANG_TIDY")
    set(_value "${BOILERPLATE_CLANG_TIDY}")
  elseif(key STREQUAL "FRAME_POINTERS")
    set(_value "${BOILERPLATE_FRAME_POINTERS}")
  elseif(key STREQUAL "DEBUG_SYMBOLS")
    set(_value "${BOILERPLATE_DEBUG_SYMBOLS}")
    if(BOILERPLATE_PROFILE MATCHES "^o3")
      set(_value ON)
    endif()
  elseif(key STREQUAL "SANITIZER")
    set(_value "${BOILERPLATE_SANITIZER}")
  elseif(key STREQUAL "LTO_MODE")
    set(_value "${BOILERPLATE_LTO_MODE}")
  elseif(key STREQUAL "PGO_MODE")
    set(_value "${BOILERPLATE_PGO_MODE}")
  elseif(key STREQUAL "PGO_DIR")
    set(_value "${BOILERPLATE_PGO_DIR}")
  elseif(key STREQUAL "PGO_PROFILE")
    set(_value "${BOILERPLATE_PGO_PROFILE}")
  elseif(key STREQUAL "ENABLE_NATIVE")
    set(_value "${BOILERPLATE_ENABLE_NATIVE}")
    if(BOILERPLATE_PROFILE MATCHES "^native-")
      set(_value ON)
    endif()
  elseif(key STREQUAL "ENABLE_NO_SEMANTIC_INTERPOSITION")
    set(_value "${BOILERPLATE_ENABLE_NO_SEMANTIC_INTERPOSITION}")
  elseif(key STREQUAL "ENABLE_GC_SECTIONS")
    set(_value "${BOILERPLATE_ENABLE_GC_SECTIONS}")
  elseif(key STREQUAL "ENABLE_NO_PLT")
    set(_value "${BOILERPLATE_ENABLE_NO_PLT}")
  elseif(key STREQUAL "USE_LLD")
    set(_value "${BOILERPLATE_USE_LLD}")
  elseif(key STREQUAL "ENABLE_ICF")
    set(_value "${BOILERPLATE_ENABLE_ICF}")
  elseif(key STREQUAL "ARTIFACT_SUFFIX")
    set(_value "${BOILERPLATE_ARTIFACT_SUFFIX}")
  else()
    message(FATAL_ERROR "Unknown boilerplate policy setting: ${key}")
  endif()
  set(${output} "${_value}" PARENT_SCOPE)
endfunction()

# Define a reusable named policy. Inherited policies must already be defined;
# later policies/explicit target options win. HOOKS are function names called
# once after the generic target settings have been applied.
function(boilerplate_define_policy name)
  get_property(_keys GLOBAL PROPERTY BOILERPLATE_POLICY_KEYS)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "${_keys}" "INHERITS;HOOKS")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES)
    message(FATAL_ERROR
      "boilerplate_define_policy(${name}): invalid arguments: ${ARG_UNPARSED_ARGUMENTS};${ARG_KEYWORDS_MISSING_VALUES}")
  endif()

  _boilerplate_policy_id("${name}" _id)
  get_property(_already GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_DEFINED")
  if(_already)
    message(FATAL_ERROR "Boilerplate policy '${name}' is already defined")
  endif()

  set(_hooks)
  foreach(_parent IN LISTS ARG_INHERITS)
    _boilerplate_policy_id("${_parent}" _parent_id)
    get_property(_parent_defined GLOBAL PROPERTY "BOILERPLATE_POLICY_${_parent_id}_DEFINED")
    if(NOT _parent_defined)
      message(FATAL_ERROR "Policy '${name}' inherits unknown policy '${_parent}'")
    endif()
    foreach(_key IN LISTS _keys)
      get_property(_set GLOBAL PROPERTY "BOILERPLATE_POLICY_${_parent_id}_${_key}" SET)
      if(_set)
        get_property(_value GLOBAL PROPERTY "BOILERPLATE_POLICY_${_parent_id}_${_key}")
        set_property(GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_${_key}" "${_value}")
      endif()
    endforeach()
    get_property(_parent_hooks GLOBAL PROPERTY "BOILERPLATE_POLICY_${_parent_id}_HOOKS")
    list(APPEND _hooks ${_parent_hooks})
  endforeach()

  foreach(_key IN LISTS _keys)
    if(DEFINED ARG_${_key})
      set_property(GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_${_key}" "${ARG_${_key}}")
    endif()
  endforeach()
  list(APPEND _hooks ${ARG_HOOKS})
  if(_hooks)
    list(REMOVE_DUPLICATES _hooks)
  endif()
  set_property(GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_HOOKS" "${_hooks}")
  set_property(GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_DEFINED" TRUE)
  set_property(GLOBAL APPEND PROPERTY BOILERPLATE_DEFINED_POLICIES "${name}")
endfunction()

# Configure effective settings for one target. This must happen before the
# generic options are applied. Defaults -> policies (left to right) -> explicit
# settings. This gives callers a small escape hatch without inventing a profile.
function(boilerplate_configure_target target)
  if(NOT TARGET ${target})
    message(FATAL_ERROR "boilerplate_configure_target: target does not exist: ${target}")
  endif()
  get_target_property(_applied ${target} BOILERPLATE_OPTIONS_APPLIED)
  get_target_property(_project_applied ${target} BOILERPLATE_PROJECT_OPTIONS_APPLIED)
  if(_applied OR _project_applied)
    message(FATAL_ERROR "Target ${target} policy cannot change after target/project options were applied")
  endif()

  get_property(_keys GLOBAL PROPERTY BOILERPLATE_POLICY_KEYS)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "${_keys}" "POLICIES;HOOKS")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES)
    message(FATAL_ERROR
      "boilerplate_configure_target(${target}): invalid arguments: ${ARG_UNPARSED_ARGUMENTS};${ARG_KEYWORDS_MISSING_VALUES}")
  endif()

  foreach(_key IN LISTS _keys)
    _boilerplate_default_setting("${_key}" _value)
    set_property(TARGET ${target} PROPERTY "BOILERPLATE_EFFECTIVE_${_key}" "${_value}")
  endforeach()

  set(_hooks)
  foreach(_policy IN LISTS ARG_POLICIES)
    _boilerplate_policy_id("${_policy}" _id)
    get_property(_defined GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_DEFINED")
    if(NOT _defined)
      get_property(_known GLOBAL PROPERTY BOILERPLATE_DEFINED_POLICIES)
      message(FATAL_ERROR "Unknown boilerplate policy '${_policy}'. Defined policies: ${_known}")
    endif()
    foreach(_key IN LISTS _keys)
      get_property(_set GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_${_key}" SET)
      if(_set)
        get_property(_value GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_${_key}")
        set_property(TARGET ${target} PROPERTY "BOILERPLATE_EFFECTIVE_${_key}" "${_value}")
      endif()
    endforeach()
    get_property(_policy_hooks GLOBAL PROPERTY "BOILERPLATE_POLICY_${_id}_HOOKS")
    list(APPEND _hooks ${_policy_hooks})
  endforeach()

  foreach(_key IN LISTS _keys)
    if(DEFINED ARG_${_key})
      set_property(TARGET ${target} PROPERTY "BOILERPLATE_EFFECTIVE_${_key}" "${ARG_${_key}}")
    endif()
  endforeach()
  list(APPEND _hooks ${ARG_HOOKS})
  if(_hooks)
    list(REMOVE_DUPLICATES _hooks)
  endif()
  set_property(TARGET ${target} PROPERTY BOILERPLATE_POLICY_HOOKS "${_hooks}")
  set_property(TARGET ${target} PROPERTY BOILERPLATE_POLICIES "${ARG_POLICIES}")
  set_property(TARGET ${target} PROPERTY BOILERPLATE_POLICY_CONFIGURED TRUE)
endfunction()

function(boilerplate_get_target_setting target key output)
  if(NOT TARGET ${target})
    message(FATAL_ERROR "boilerplate_get_target_setting: target does not exist: ${target}")
  endif()
  get_target_property(_configured ${target} BOILERPLATE_POLICY_CONFIGURED)
  if(NOT _configured)
    boilerplate_configure_target(${target})
  endif()
  get_property(_keys GLOBAL PROPERTY BOILERPLATE_POLICY_KEYS)
  if(NOT key IN_LIST _keys)
    message(FATAL_ERROR "Unknown boilerplate target setting '${key}'")
  endif()
  get_target_property(_value ${target} "BOILERPLATE_EFFECTIVE_${key}")
  set(${output} "${_value}" PARENT_SCOPE)
endfunction()

function(boilerplate_apply_policy_hooks target)
  get_target_property(_hooks ${target} BOILERPLATE_POLICY_HOOKS)
  foreach(_hook IN LISTS _hooks)
    if(NOT COMMAND ${_hook})
      message(FATAL_ERROR "Policy hook '${_hook}' for target ${target} is not a CMake command")
    endif()
    cmake_language(CALL ${_hook} ${target})
  endforeach()
endfunction()

# Neutral baseline: enough for an ordinary std-only C++ target, and explicitly
# resets expensive/project-specific defaults inherited from a larger build.
boilerplate_define_policy(minimal
  CUDA OFF
  CUDA_LINEINFO OFF
  CUDA_DEVICE_DEBUG OFF
  CUDA_FAST_MATH OFF
  WARNINGS ON
  WARNINGS_AS_ERRORS OFF
  HARDENING OFF
  CFI none
  CFI_DIAGNOSTICS OFF
  WINDOWS_CFG OFF
  COVERAGE OFF
  REPRODUCIBLE OFF
  UNITY_BUILD OFF
  CCACHE OFF
  CLANG_TIDY OFF
  FRAME_POINTERS OFF
  DEBUG_SYMBOLS OFF
  SANITIZER none
  LTO_MODE none
  PGO_MODE none
  ENABLE_NATIVE OFF
  ENABLE_NO_SEMANTIC_INTERPOSITION OFF
  ENABLE_GC_SECTIONS OFF
  ENABLE_NO_PLT OFF
  USE_LLD OFF
  ENABLE_ICF OFF)

# Small orthogonal building blocks. Domain policies (runtime, Qt, graphics, ...)
# can inherit/compose these without becoming part of this generic substrate.
boilerplate_define_policy(cpp-minimal INHERITS minimal)
boilerplate_define_policy(hardened HARDENING ON)
boilerplate_define_policy(hardening INHERITS hardened)
boilerplate_define_policy(coverage COVERAGE ON)
boilerplate_define_policy(reproducible REPRODUCIBLE ON)
boilerplate_define_policy(debug-symbols DEBUG_SYMBOLS ON)
boilerplate_define_policy(werror WARNINGS_AS_ERRORS ON)
boilerplate_define_policy(ccache CCACHE ON)
boilerplate_define_policy(clang-tidy CLANG_TIDY ON)
boilerplate_define_policy(frame-pointers FRAME_POINTERS ON)
boilerplate_define_policy(unity UNITY_BUILD ON)
boilerplate_define_policy(native ENABLE_NATIVE ON)
boilerplate_define_policy(thin-lto LTO_MODE thin)
boilerplate_define_policy(full-lto LTO_MODE full)
boilerplate_define_policy(lld USE_LLD ON)
boilerplate_define_policy(cfi INHERITS thin-lto lld CFI cfi)
boilerplate_define_policy(cfi-icall INHERITS thin-lto lld CFI cfi-icall)
boilerplate_define_policy(cfi-vcall INHERITS thin-lto lld CFI cfi-vcall)
boilerplate_define_policy(windows-cfg WINDOWS_CFG ON)
boilerplate_define_policy(gc-sections ENABLE_GC_SECTIONS ON)
boilerplate_define_policy(no-plt ENABLE_NO_PLT ON)
boilerplate_define_policy(no-semantic-interposition ENABLE_NO_SEMANTIC_INTERPOSITION ON)
boilerplate_define_policy(icf ENABLE_ICF ON)
boilerplate_define_policy(asan SANITIZER address)
boilerplate_define_policy(ubsan SANITIZER undefined)
boilerplate_define_policy(asan-ubsan SANITIZER address-undefined)
boilerplate_define_policy(tsan SANITIZER thread)
boilerplate_define_policy(lsan SANITIZER leak)
boilerplate_define_policy(pgo-generate PGO_MODE generate)
boilerplate_define_policy(pgo-use PGO_MODE use)

function(boilerplate_list_policies output)
  get_property(_policies GLOBAL PROPERTY BOILERPLATE_DEFINED_POLICIES)
  set(${output} "${_policies}" PARENT_SCOPE)
endfunction()

function(boilerplate_print_target_policy target)
  if(NOT TARGET ${target})
    message(FATAL_ERROR "boilerplate_print_target_policy: target does not exist: ${target}")
  endif()
  get_target_property(_policies ${target} BOILERPLATE_POLICIES)
  if(NOT _policies)
    set(_policies "<project defaults>")
  endif()
  message(STATUS "Boilerplate target ${target}: policies=${_policies}")
  get_property(_keys GLOBAL PROPERTY BOILERPLATE_POLICY_KEYS)
  foreach(_key IN LISTS _keys)
    boilerplate_get_target_setting(${target} ${_key} _value)
    message(STATUS "  ${_key}=${_value}")
  endforeach()
endfunction()
