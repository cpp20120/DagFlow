include_guard(GLOBAL)

# Opt-in integration; CMake/ninja builds native targets, C# owns the subsequent execution.
set(DAGFLOW_TOOLS_EXECUTABLE "" CACHE FILEPATH "Optional published DagFlow.Tools CLI binary")
set(DAGFLOW_TOOLS_BRIDGE_FUZZER "dagflow_fuzz_queues" CACHE STRING
    "A single fuzzer to expose through the bridge PoC")

function(dagflow_add_target_bridge bridge_name)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "" "TARGET" "ARGS")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES OR NOT ARG_TARGET OR NOT TARGET "${ARG_TARGET}")
    message(FATAL_ERROR "dagflow_add_target_bridge(${bridge_name}): TARGET must exist")
  endif()
  if(NOT DAGFLOW_TOOLS_EXECUTABLE OR NOT EXISTS "${DAGFLOW_TOOLS_EXECUTABLE}")
    message(FATAL_ERROR "Set DAGFLOW_TOOLS_EXECUTABLE to the published DagFlow.Tools binary")
  endif()
  if(CMAKE_CROSSCOMPILING)
    message(FATAL_ERROR "The bridge PoC supports native execution only; DagFlow's emulator support is not yet exported")
  endif()
  get_target_property(_type "${ARG_TARGET}" TYPE)
  if(NOT _type STREQUAL "EXECUTABLE")
    message(FATAL_ERROR "Only CMake executable targets are supported by the bridge PoC")
  endif()

  # The first configure works on CMake 3.26: unlike a File API query created
  # mid-configure, file(GENERATE) writes this manifest in the current generate.
  # Generator expressions preserve OUTPUT_NAME, per-config output directories,
  # executable suffixes and IDE multi-config generators.
  set(_manifest "${CMAKE_BINARY_DIR}/dagflow-tools/targets/$<CONFIG>/${ARG_TARGET}.json")
  set(_json "{\n  \"schemaVersion\": 1,\n  \"target\": \"${ARG_TARGET}\",\n  \"configuration\": \"$<CONFIG>\",\n  \"executable\": \"$<TARGET_FILE:${ARG_TARGET}>\"\n}\n")
  file(GENERATE OUTPUT "${_manifest}" CONTENT "${_json}")

  add_custom_target("${bridge_name}"
    COMMAND "${DAGFLOW_TOOLS_EXECUTABLE}" target run
      --build-dir "${CMAKE_BINARY_DIR}"
      --target "${ARG_TARGET}"
      --config "$<CONFIG>"
      --prepared -- ${ARG_ARGS}
    DEPENDS "${ARG_TARGET}"
    USES_TERMINAL
    VERBATIM
  )
  set_property(TARGET "${bridge_name}" PROPERTY FOLDER "dagflow/tools")
endfunction()
