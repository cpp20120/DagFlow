include_guard(GLOBAL)

set(DAGFLOW_HARNESS_RESULTS_DIR "${CMAKE_BINARY_DIR}/harness-results" CACHE PATH
  "Output root for the optional Python process harness")
set(DAGFLOW_HARNESS_SCRIPT "${CMAKE_CURRENT_LIST_DIR}/harness/run.py" CACHE FILEPATH
  "Path to the dagflow process harness entry point")

function(_dagflow_harness_append_list output flag)
  set(_result "${${output}}")
  foreach(_value IN LISTS ARGN)
    list(APPEND _result "${flag}=${_value}")
  endforeach()
  set(${output} "${_result}" PARENT_SCOPE)
endfunction()

# Register an explicit process benchmark/stress harness without making Python a
# dependency of ordinary builds. The target is built by CMake; Python owns only
# execution, provenance, affinity, logs and statistics.
function(dagflow_add_harness name)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "RANDOMIZE;PERF" 
    "TARGET;CASES;PARSER;ROUNDS;WARMUP_RUNS;TIMEOUT;OUTPUT_DIR;WORKING_DIRECTORY;AFFINITY;CPU_COUNT;CPUS;SEED"
    "ARGS;ENVIRONMENT;METRICS;INVARIANTS;PERF_EVENTS;METADATA;LABELS")
  if(ARG_UNPARSED_ARGUMENTS OR ARG_KEYWORDS_MISSING_VALUES OR NOT ARG_TARGET OR NOT TARGET ${ARG_TARGET})
    message(FATAL_ERROR "dagflow_add_harness(${name}): TARGET must name an existing target; invalid arguments: ${ARG_UNPARSED_ARGUMENTS};${ARG_KEYWORDS_MISSING_VALUES}")
  endif()
  get_target_property(_type ${ARG_TARGET} TYPE)
  if(NOT _type STREQUAL "EXECUTABLE")
    message(FATAL_ERROR "dagflow_add_harness(${name}): TARGET must be an executable")
  endif()
  if(ARG_CASES AND ARG_ARGS)
    message(FATAL_ERROR "dagflow_add_harness(${name}): use CASES or ARGS, not both")
  endif()
  if(ARG_CASES)
    get_filename_component(ARG_CASES "${ARG_CASES}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
    if(NOT EXISTS "${ARG_CASES}")
      message(FATAL_ERROR "dagflow_add_harness(${name}): cases file does not exist: ${ARG_CASES}")
    endif()
  endif()
  if(NOT ARG_PARSER)
    set(ARG_PARSER json-line)
  endif()
  if(NOT ARG_PARSER MATCHES "^(json-line|json|none)$")
    message(FATAL_ERROR "dagflow_add_harness(${name}): invalid PARSER=${ARG_PARSER}")
  endif()
  if(NOT ARG_ROUNDS)
    set(ARG_ROUNDS 1)
  endif()
  if(NOT DEFINED ARG_WARMUP_RUNS)
    set(ARG_WARMUP_RUNS 0)
  endif()
  if(NOT ARG_TIMEOUT)
    set(ARG_TIMEOUT 60)
  endif()
  if(NOT ARG_AFFINITY)
    set(ARG_AFFINITY inherit)
  endif()
  if(NOT ARG_AFFINITY MATCHES "^(inherit|physical|none)$")
    message(FATAL_ERROR "dagflow_add_harness(${name}): invalid AFFINITY=${ARG_AFFINITY}")
  endif()
  if(NOT ARG_CPU_COUNT)
    set(ARG_CPU_COUNT 0)
  endif()
  if(NOT ARG_SEED)
    set(ARG_SEED 0xC0FFEE)
  endif()
  if(NOT ARG_ROUNDS MATCHES "^[1-9][0-9]*$" OR NOT ARG_WARMUP_RUNS MATCHES "^[0-9]+$"
      OR NOT ARG_TIMEOUT MATCHES "^[0-9]+([.][0-9]+)?$" OR NOT ARG_CPU_COUNT MATCHES "^[0-9]+$")
    message(FATAL_ERROR "dagflow_add_harness(${name}): invalid rounds/warmup/timeout/cpu-count")
  endif()

  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  if(NOT EXISTS "${DAGFLOW_HARNESS_SCRIPT}")
    message(FATAL_ERROR "DagFlow harness script not found: ${DAGFLOW_HARNESS_SCRIPT}")
  endif()
  if(NOT ARG_OUTPUT_DIR)
    set(ARG_OUTPUT_DIR "${DAGFLOW_HARNESS_RESULTS_DIR}/$<CONFIG>/${name}")
  endif()

  set(_command "${CMAKE_COMMAND}" -E env PYTHONDONTWRITEBYTECODE=1
    "${Python3_EXECUTABLE}" "${DAGFLOW_HARNESS_SCRIPT}" run
    --name "${name}"
    --binary "$<TARGET_FILE:${ARG_TARGET}>"
    --output "${ARG_OUTPUT_DIR}"
    --parser "${ARG_PARSER}"
    --rounds "${ARG_ROUNDS}"
    --warmup-runs "${ARG_WARMUP_RUNS}"
    --timeout "${ARG_TIMEOUT}"
    --affinity "${ARG_AFFINITY}"
    --cpu-count "${ARG_CPU_COUNT}"
    --seed "${ARG_SEED}"
    --overwrite)
  if(ARG_CASES)
    list(APPEND _command --cases "${ARG_CASES}")
  else()
    _dagflow_harness_append_list(_command --arg ${ARG_ARGS})
  endif()
  if(ARG_WORKING_DIRECTORY)
    get_filename_component(_workdir "${ARG_WORKING_DIRECTORY}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
    list(APPEND _command --working-directory "${_workdir}")
  endif()
  if(ARG_CPUS)
    list(APPEND _command --cpus "${ARG_CPUS}")
  endif()
  if(ARG_RANDOMIZE)
    list(APPEND _command --randomize)
  endif()
  _dagflow_harness_append_list(_command --env ${ARG_ENVIRONMENT})
  _dagflow_harness_append_list(_command --metric ${ARG_METRICS})
  _dagflow_harness_append_list(_command --invariant ${ARG_INVARIANTS})
  if(ARG_PERF OR ARG_PERF_EVENTS)
    _dagflow_harness_append_list(_command --perf-event ${ARG_PERF_EVENTS})
  endif()

  get_target_property(_policies ${ARG_TARGET} DAGFLOW_POLICIES)
  if(NOT _policies)
    set(_policies "project-defaults")
  else()
    string(REPLACE ";" "," _policies "${_policies}")
  endif()
  list(APPEND _command "--meta=target=${ARG_TARGET}" "--meta=policies=${_policies}"
    "--meta=compiler=${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}"
    "--meta=generator=${CMAKE_GENERATOR}" "--meta=project=${PROJECT_NAME}")
  foreach(_setting IN ITEMS CXX_STANDARD SANITIZER CFI CFI_DIAGNOSTICS WINDOWS_CFG LTO_MODE PGO_MODE ENABLE_NATIVE
      ENABLE_NO_SEMANTIC_INTERPOSITION ENABLE_GC_SECTIONS ENABLE_NO_PLT USE_LLD ENABLE_ICF FRAME_POINTERS REPRODUCIBLE HARDENING COVERAGE)
    dagflow_get_target_setting(${ARG_TARGET} ${_setting} _value)
    list(APPEND _command "--meta=${_setting}=${_value}")
  endforeach()
  _dagflow_harness_append_list(_command --meta ${ARG_METADATA})

  add_custom_target(run_${name}
    COMMAND ${_command}
    DEPENDS ${ARG_TARGET}
    USES_TERMINAL VERBATIM)
  # Deliberately no aggregate run target: parallel build schedulers could run
  # independent measurements concurrently and invalidate the experiment.
  set_property(TARGET run_${name} PROPERTY FOLDER "dagflow/harness")
endfunction()
