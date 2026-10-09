set(DAGFLOW_RUNTIME_SOURCES src/thread_pool.cpp src/scheduler.cpp src/parking_lot.cpp
      src/idle_accounting.cpp src/task_graph.cpp src/task_scope.cpp
      src/runtime_memory.cpp src/runtime_diagnostics.cpp)

# Resolve allocator dependencies only for targets that compile runtime memory.
set_property(CACHE DAGFLOW_ALLOCATOR PROPERTY STRINGS mimalloc tbbmalloc system)
if(NOT DAGFLOW_ALLOCATOR MATCHES "^(system|mimalloc|tbbmalloc)$")
  message(FATAL_ERROR "Unsupported DAGFLOW_ALLOCATOR: ${DAGFLOW_ALLOCATOR}")
endif()
if(DAGFLOW_BUILD_SHARED OR DAGFLOW_BUILD_STATIC OR DAGFLOW_BUILD_TESTS OR DAGFLOW_BUILD_FUZZERS)
  if(DAGFLOW_ALLOCATOR STREQUAL "mimalloc")
    find_package(mimalloc CONFIG REQUIRED)
  elseif(DAGFLOW_ALLOCATOR STREQUAL "tbbmalloc")
    find_package(TBB CONFIG REQUIRED COMPONENTS tbbmalloc)
  endif()
endif()

function(dagflow_apply_allocator target)
  if(DAGFLOW_ALLOCATOR STREQUAL "mimalloc")
    target_compile_definitions(${target} PRIVATE DAGFLOW_USE_MIMALLOC)
    target_link_libraries(${target} PRIVATE mimalloc)
  elseif(DAGFLOW_ALLOCATOR STREQUAL "tbbmalloc")
    target_compile_definitions(${target} PRIVATE DAGFLOW_USE_TBBMALLOC)
    target_link_libraries(${target} PRIVATE TBB::tbbmalloc)
  endif()
endfunction()

# Use the framework's shared/static library construction and package exports.
set(DAGFLOW_TARGET "")
if(DAGFLOW_BUILD_SHARED OR DAGFLOW_BUILD_STATIC)
  boilerplate_add_library(DagFlow VERSION ${PROJECT_VERSION}
    SOURCES ${DAGFLOW_RUNTIME_SOURCES}
    INCLUDE_DIR include PUBLIC_LIBRARIES Threads::Threads
    PACKAGE_CONFIG cmake/DagFlowConfig.cmake.in)
  foreach(_kind IN ITEMS shared static)
    if(TARGET DagFlow_${_kind})
      set(_target DagFlow_${_kind})
      # Existing public API has no export annotations. Retain its visibility
      # and Windows auto-export contract while adopting the library helper.
      set_target_properties(${_target} PROPERTIES CXX_VISIBILITY_PRESET default
        WINDOWS_EXPORT_ALL_SYMBOLS ON)
      # Match the documented build-tree and installed package target names.
      if(NOT TARGET DagFlow::DagFlow_${_kind})
        add_library(DagFlow::DagFlow_${_kind} ALIAS ${_target})
      endif()
      target_compile_features(${_target} PUBLIC cxx_std_23)
      boilerplate_set_output_name(${_target} dagflow)
      dagflow_apply_allocator(${_target})
      if(DAGFLOW_RUNTIME_DIAGNOSTICS)
        target_compile_definitions(${_target} PUBLIC DAGFLOW_RUNTIME_DIAGNOSTICS=1)
      endif()
    endif()
  endforeach()
  if(DAGFLOW_BUILD_SHARED)
    set(DAGFLOW_TARGET DagFlow_shared)
  else()
    set(DAGFLOW_TARGET DagFlow_static)
  endif()
  if(DAGFLOW_BUILD_STATIC)
    target_compile_definitions(DagFlow_static PRIVATE DAGFLOW_STATIC)
    # Windows import libraries and static archives must not overwrite each other.
    if(WIN32 AND DAGFLOW_BUILD_SHARED)
      boilerplate_set_output_name(DagFlow_static dagflow-static)
    endif()
  endif()
endif()
