# These options must be resolved before project(): vcpkg installs the selected
# manifest features while loading its toolchain, before compiler detection.
option(DAGFLOW_BUILD_SHARED "Build DagFlow as a shared library" ON)
option(DAGFLOW_BUILD_STATIC "Build DagFlow as a static library" OFF)
option(DAGFLOW_BUILD_BENCH "Build the oneTBB comparison benchmark" OFF)
option(DAGFLOW_BUILD_TESTS "Build tests" OFF)
option(DAGFLOW_BUILD_FUZZERS "Build coverage-guided libFuzzer targets" OFF)
set(DAGFLOW_ALLOCATOR "system" CACHE STRING "Runtime allocator: mimalloc, tbbmalloc, or system")

# An embedded library must never replace its parent's toolchain or manifest.
if(NOT CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  return()
endif()

set(_dagflow_features)
if(DAGFLOW_BUILD_BENCH)
  list(APPEND _dagflow_features tbb-bench)
endif()
if(DAGFLOW_BUILD_SHARED OR DAGFLOW_BUILD_STATIC OR DAGFLOW_BUILD_TESTS OR DAGFLOW_BUILD_FUZZERS)
  if(DAGFLOW_ALLOCATOR MATCHES "^(mimalloc|tbbmalloc)$")
    list(APPEND _dagflow_features "${DAGFLOW_ALLOCATOR}")
  endif()
endif()

set(DAGFLOW_DEPENDENCY_PROVIDER auto CACHE STRING "Dependency provider: auto, system, vcpkg, none, fetchcontent, cpm")
option(DAGFLOW_VCPKG_BOOTSTRAP "Provision pinned vcpkg when no explicit root/toolchain is supplied" ON)
if(DAGFLOW_DEPENDENCY_PROVIDER STREQUAL "auto")
  # Keep auto in the cache so reconfiguring the same tree recalculates features.
  set(DAGFLOW_DEPENDENCY_PROVIDER system)
  if(_dagflow_features)
    set(DAGFLOW_DEPENDENCY_PROVIDER vcpkg)
  endif()
endif()
if(DAGFLOW_DEPENDENCY_PROVIDER STREQUAL "vcpkg")
  # Do not cache the derived list: switching allocators must remove old features.
  set(VCPKG_MANIFEST_FEATURES ${VCPKG_MANIFEST_FEATURES} ${_dagflow_features})
  list(REMOVE_DUPLICATES VCPKG_MANIFEST_FEATURES)
endif()
