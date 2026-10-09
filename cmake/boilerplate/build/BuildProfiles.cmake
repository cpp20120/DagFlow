include_guard(GLOBAL)

# Optimization / experiment switches. These are intentionally orthogonal so
# presets can compose sane distribution builds and deliberately extreme local
# benchmark builds from the same CMakeLists.txt.
set(BOILERPLATE_LTO_MODE "none" CACHE STRING "LTO mode: none, thin, or full")
set_property(CACHE BOILERPLATE_LTO_MODE PROPERTY STRINGS none thin full)
option(BOILERPLATE_ENABLE_NATIVE "Optimize for the build machine CPU (-march=native)" OFF)
option(BOILERPLATE_ENABLE_NO_SEMANTIC_INTERPOSITION "Disable ELF semantic interposition" OFF)
option(BOILERPLATE_ENABLE_GC_SECTIONS "Put code/data in individual sections and GC unused sections" OFF)
option(BOILERPLATE_ENABLE_NO_PLT "Avoid PLT calls on supported ELF targets" OFF)
option(BOILERPLATE_USE_LLD "Use lld for supported GNU-style compiler drivers" OFF)
option(BOILERPLATE_ENABLE_ICF "Enable safe identical-code folding with lld" OFF)

# PGO is separate from LTO. Clang uses LLVM instrumentation profiles; GCC uses
# gcov-style profiles. Explicit CMake targets drive training and merging.
set(BOILERPLATE_PGO_MODE "none" CACHE STRING "PGO mode: none, generate, or use")
set_property(CACHE BOILERPLATE_PGO_MODE PROPERTY STRINGS none generate use)
set(BOILERPLATE_PGO_DIR "${CMAKE_BINARY_DIR}/pgo" CACHE PATH "Directory for raw/generated PGO data")
set(BOILERPLATE_PGO_PROFILE "" CACHE FILEPATH "Merged Clang .profdata file used by PGO=use")

# Distribution presets use suffixes when collecting artifacts in one directory.
set(BOILERPLATE_ARTIFACT_SUFFIX "" CACHE STRING "Suffix appended to produced binary/library names")

# Named build profiles are defined here; custom keeps the orthogonal switches.
set(BOILERPLATE_PROFILE "custom" CACHE STRING "Named build profile; custom uses individual switches")
set(_boilerplate_profiles custom debug relwithdebinfo release lto full-lto
    native-thinlto native-full-lto o3 o3-lto
    pgo-generate pgo-use lto-pgo-generate lto-pgo-use
    native-full-lto-pgo-generate native-full-lto-pgo-use)
set_property(CACHE BOILERPLATE_PROFILE PROPERTY STRINGS ${_boilerplate_profiles})
if(NOT BOILERPLATE_PROFILE IN_LIST _boilerplate_profiles)
  message(FATAL_ERROR "Unknown BOILERPLATE_PROFILE=${BOILERPLATE_PROFILE}; expected ${_boilerplate_profiles}")
endif()
option(BOILERPLATE_DEBUG_SYMBOLS "Include debug symbols in optimized targets" OFF)
set(BOILERPLATE_SANITIZER "none" CACHE STRING "Sanitizer: none, address, undefined, address-undefined, thread, leak")
set_property(CACHE BOILERPLATE_SANITIZER PROPERTY STRINGS none address undefined address-undefined thread leak)
if(NOT BOILERPLATE_SANITIZER MATCHES "^(none|address|undefined|address-undefined|thread|leak)$")
  message(FATAL_ERROR "Unknown BOILERPLATE_SANITIZER=${BOILERPLATE_SANITIZER}")
endif()

if(NOT BOILERPLATE_PROFILE STREQUAL "custom")
  set(CMAKE_BUILD_TYPE Release)
  set(BOILERPLATE_LTO_MODE none)
  set(BOILERPLATE_PGO_MODE none)
  if(BOILERPLATE_PROFILE STREQUAL "debug")
    set(CMAKE_BUILD_TYPE Debug)
  elseif(BOILERPLATE_PROFILE STREQUAL "relwithdebinfo")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
  endif()
  if(BOILERPLATE_PROFILE MATCHES "^(lto|native-thinlto|lto-pgo-generate|lto-pgo-use)$")
    set(BOILERPLATE_LTO_MODE thin)
  elseif(BOILERPLATE_PROFILE MATCHES "^(full-lto|native-full-lto|native-full-lto-pgo-generate|native-full-lto-pgo-use|o3-lto)$")
    set(BOILERPLATE_LTO_MODE full)
  endif()
  if(BOILERPLATE_PROFILE MATCHES "pgo-generate$")
    set(BOILERPLATE_PGO_MODE generate)
  elseif(BOILERPLATE_PROFILE MATCHES "pgo-use$")
    set(BOILERPLATE_PGO_MODE use)
  endif()
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Build type selected by BOILERPLATE_PROFILE" FORCE)
  if(CMAKE_CONFIGURATION_TYPES)
    # A named profile fixes its configuration for Visual Studio/Xcode too.
    set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE}" CACHE STRING "Configuration selected by BOILERPLATE_PROFILE" FORCE)
  endif()
  set(BOILERPLATE_LTO_MODE "${BOILERPLATE_LTO_MODE}" CACHE STRING "LTO mode selected by BOILERPLATE_PROFILE" FORCE)
  set(BOILERPLATE_PGO_MODE "${BOILERPLATE_PGO_MODE}" CACHE STRING "PGO mode selected by BOILERPLATE_PROFILE" FORCE)
endif()

if(NOT BOILERPLATE_LTO_MODE MATCHES "^(none|thin|full)$" OR
   NOT BOILERPLATE_PGO_MODE MATCHES "^(none|generate|use)$")
  message(FATAL_ERROR "Invalid BOILERPLATE_LTO_MODE or BOILERPLATE_PGO_MODE")
endif()
if(BOILERPLATE_LTO_MODE STREQUAL "thin" AND (MSVC OR NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
  message(FATAL_ERROR "ThinLTO requires a GNU-style Clang driver; select full-lto for GCC/MSVC")
endif()
if(NOT BOILERPLATE_SANITIZER STREQUAL "none" AND MSVC)
  message(FATAL_ERROR "BOILERPLATE_SANITIZER presets currently support GCC/Clang on Unix")
endif()
if(MSVC AND (NOT BOILERPLATE_PGO_MODE STREQUAL "none" OR BOILERPLATE_ENABLE_NATIVE
    OR BOILERPLATE_USE_LLD OR BOILERPLATE_ENABLE_ICF OR BOILERPLATE_DEBUG_SYMBOLS
    OR BOILERPLATE_PROFILE MATCHES "^(native-|o3)"))
  message(FATAL_ERROR "Native/PGO/lld/ICF/extra symbols profiles require a GNU-style GCC/Clang driver")
endif()
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|MSVC")
  if(NOT BOILERPLATE_LTO_MODE STREQUAL "none" OR NOT BOILERPLATE_PGO_MODE STREQUAL "none"
      OR NOT BOILERPLATE_SANITIZER STREQUAL "none" OR BOILERPLATE_ENABLE_NATIVE)
    message(FATAL_ERROR "Selected optimization profile is unsupported by this compiler")
  endif()
endif()
if(NOT MSVC AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  if(BOILERPLATE_ENABLE_ICF AND NOT BOILERPLATE_USE_LLD)
    message(FATAL_ERROR "BOILERPLATE_ENABLE_ICF requires BOILERPLATE_USE_LLD=ON")
  endif()
  if(BOILERPLATE_PGO_MODE STREQUAL "use" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if(NOT BOILERPLATE_PGO_PROFILE OR NOT EXISTS "${BOILERPLATE_PGO_PROFILE}")
      message(FATAL_ERROR
        "Clang PGO use requires BOILERPLATE_PGO_PROFILE to point at an existing .profdata file")
    endif()
  endif()
endif()
message(STATUS "Boilerplate: profile=${BOILERPLATE_PROFILE}, build=${CMAKE_BUILD_TYPE}, LTO=${BOILERPLATE_LTO_MODE}, PGO=${BOILERPLATE_PGO_MODE}, sanitizer=${BOILERPLATE_SANITIZER}")
