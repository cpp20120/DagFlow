include_guard(GLOBAL)

# Optimization / experiment switches. These are intentionally orthogonal so
# presets can compose sane distribution builds and deliberately extreme local
# benchmark builds from the same CMakeLists.txt.
set(DAGFLOW_LTO_MODE "none" CACHE STRING "LTO mode: none, thin, or full")
set_property(CACHE DAGFLOW_LTO_MODE PROPERTY STRINGS none thin full)
option(DAGFLOW_ENABLE_NATIVE "Optimize for the build machine CPU (-march=native)" OFF)
option(DAGFLOW_ENABLE_NO_SEMANTIC_INTERPOSITION "Disable ELF semantic interposition" OFF)
option(DAGFLOW_ENABLE_GC_SECTIONS "Put code/data in individual sections and GC unused sections" OFF)
option(DAGFLOW_ENABLE_NO_PLT "Avoid PLT calls on supported ELF targets" OFF)
option(DAGFLOW_USE_LLD "Use lld for supported GNU-style compiler drivers" OFF)
option(DAGFLOW_ENABLE_ICF "Enable safe identical-code folding with lld" OFF)

# PGO is separate from LTO. Clang uses LLVM instrumentation profiles; GCC uses
# gcov-style profiles. Explicit CMake targets drive training and merging.
set(DAGFLOW_PGO_MODE "none" CACHE STRING "PGO mode: none, generate, or use")
set_property(CACHE DAGFLOW_PGO_MODE PROPERTY STRINGS none generate use)
set(DAGFLOW_PGO_DIR "${CMAKE_BINARY_DIR}/pgo" CACHE PATH "Directory for raw/generated PGO data")
set(DAGFLOW_PGO_PROFILE "" CACHE FILEPATH "Merged Clang .profdata file used by PGO=use")

# Distribution presets use suffixes when collecting artifacts in one directory.
set(DAGFLOW_ARTIFACT_SUFFIX "" CACHE STRING "Suffix appended to produced binary/library names")

# Named build profiles are defined here; custom keeps the orthogonal switches.
set(DAGFLOW_PROFILE "custom" CACHE STRING "Named build profile; custom uses individual switches")
set(_dagflow_profiles custom debug relwithdebinfo release lto full-lto
    native-thinlto native-full-lto o3 o3-lto
    pgo-generate pgo-use lto-pgo-generate lto-pgo-use
    native-full-lto-pgo-generate native-full-lto-pgo-use)
set_property(CACHE DAGFLOW_PROFILE PROPERTY STRINGS ${_dagflow_profiles})
if(NOT DAGFLOW_PROFILE IN_LIST _dagflow_profiles)
  message(FATAL_ERROR "Unknown DAGFLOW_PROFILE=${DAGFLOW_PROFILE}; expected ${_dagflow_profiles}")
endif()
option(DAGFLOW_DEBUG_SYMBOLS "Include debug symbols in optimized targets" OFF)
set(DAGFLOW_SANITIZER "none" CACHE STRING "Sanitizer: none, address, undefined, address-undefined, thread, leak")
set_property(CACHE DAGFLOW_SANITIZER PROPERTY STRINGS none address undefined address-undefined thread leak)
if(NOT DAGFLOW_SANITIZER MATCHES "^(none|address|undefined|address-undefined|thread|leak)$")
  message(FATAL_ERROR "Unknown DAGFLOW_SANITIZER=${DAGFLOW_SANITIZER}")
endif()

if(NOT DAGFLOW_PROFILE STREQUAL "custom")
  set(CMAKE_BUILD_TYPE Release)
  set(DAGFLOW_LTO_MODE none)
  set(DAGFLOW_PGO_MODE none)
  if(DAGFLOW_PROFILE STREQUAL "debug")
    set(CMAKE_BUILD_TYPE Debug)
  elseif(DAGFLOW_PROFILE STREQUAL "relwithdebinfo")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
  endif()
  if(DAGFLOW_PROFILE MATCHES "^(lto|native-thinlto|lto-pgo-generate|lto-pgo-use)$")
    set(DAGFLOW_LTO_MODE thin)
  elseif(DAGFLOW_PROFILE MATCHES "^(full-lto|native-full-lto|native-full-lto-pgo-generate|native-full-lto-pgo-use|o3-lto)$")
    set(DAGFLOW_LTO_MODE full)
  endif()
  if(DAGFLOW_PROFILE MATCHES "pgo-generate$")
    set(DAGFLOW_PGO_MODE generate)
  elseif(DAGFLOW_PROFILE MATCHES "pgo-use$")
    set(DAGFLOW_PGO_MODE use)
  endif()
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Build type selected by DAGFLOW_PROFILE" FORCE)
  if(CMAKE_CONFIGURATION_TYPES)
    # A named profile fixes its configuration for Visual Studio/Xcode too.
    set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE}" CACHE STRING "Configuration selected by DAGFLOW_PROFILE" FORCE)
  endif()
  set(DAGFLOW_LTO_MODE "${DAGFLOW_LTO_MODE}" CACHE STRING "LTO mode selected by DAGFLOW_PROFILE" FORCE)
  set(DAGFLOW_PGO_MODE "${DAGFLOW_PGO_MODE}" CACHE STRING "PGO mode selected by DAGFLOW_PROFILE" FORCE)
endif()

if(NOT DAGFLOW_LTO_MODE MATCHES "^(none|thin|full)$" OR
   NOT DAGFLOW_PGO_MODE MATCHES "^(none|generate|use)$")
  message(FATAL_ERROR "Invalid DAGFLOW_LTO_MODE or DAGFLOW_PGO_MODE")
endif()
if(DAGFLOW_LTO_MODE STREQUAL "thin" AND (MSVC OR NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
  message(FATAL_ERROR "ThinLTO requires a GNU-style Clang driver; select full-lto for GCC/MSVC")
endif()
if(NOT DAGFLOW_SANITIZER STREQUAL "none" AND MSVC)
  message(FATAL_ERROR "DAGFLOW_SANITIZER presets currently support GCC/Clang on Unix")
endif()
if(MSVC AND (NOT DAGFLOW_PGO_MODE STREQUAL "none" OR DAGFLOW_ENABLE_NATIVE
    OR DAGFLOW_USE_LLD OR DAGFLOW_ENABLE_ICF OR DAGFLOW_DEBUG_SYMBOLS
    OR DAGFLOW_PROFILE MATCHES "^(native-|o3)"))
  message(FATAL_ERROR "Native/PGO/lld/ICF/extra symbols profiles require a GNU-style GCC/Clang driver")
endif()
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|MSVC")
  if(NOT DAGFLOW_LTO_MODE STREQUAL "none" OR NOT DAGFLOW_PGO_MODE STREQUAL "none"
      OR NOT DAGFLOW_SANITIZER STREQUAL "none" OR DAGFLOW_ENABLE_NATIVE)
    message(FATAL_ERROR "Selected optimization profile is unsupported by this compiler")
  endif()
endif()
if(NOT MSVC AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  if(DAGFLOW_ENABLE_ICF AND NOT DAGFLOW_USE_LLD)
    message(FATAL_ERROR "DAGFLOW_ENABLE_ICF requires DAGFLOW_USE_LLD=ON")
  endif()
  if(DAGFLOW_PGO_MODE STREQUAL "use" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if(NOT DAGFLOW_PGO_PROFILE OR NOT EXISTS "${DAGFLOW_PGO_PROFILE}")
      message(FATAL_ERROR
        "Clang PGO use requires DAGFLOW_PGO_PROFILE to point at an existing .profdata file")
    endif()
  endif()
endif()
message(STATUS "DagFlow: profile=${DAGFLOW_PROFILE}, build=${CMAKE_BUILD_TYPE}, LTO=${DAGFLOW_LTO_MODE}, PGO=${DAGFLOW_PGO_MODE}, sanitizer=${DAGFLOW_SANITIZER}")
