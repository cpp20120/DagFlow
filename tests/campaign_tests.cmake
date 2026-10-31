cmake_minimum_required(VERSION 3.26)
include("${CONFIG}")
execute_process(COMMAND "${CMAKE_COMMAND}" "-DHARNESS_CONFIG=${CONFIG}"
  -P "${SOURCE_DIR}/cmake/dagflow/benchmark/HarnessCMake.cmake"
  RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err TIMEOUT 30)
if(NOT "${rc}" STREQUAL "0")
  message(FATAL_ERROR "Native campaign failed: ${out}\n${err}")
endif()
file(READ "${HARNESS_OUTPUT}/summary.json" summary)
file(READ "${HARNESS_OUTPUT}/manifest.json" manifest)
file(SHA256 "${HARNESS_OUTPUT}/manifest.json" manifest_sha256)
string(JSON summary_manifest_sha256 GET "${summary}" manifest_sha256)
if(NOT summary_manifest_sha256 STREQUAL manifest_sha256)
  message(FATAL_ERROR "Campaign summary does not identify its manifest")
endif()
string(JSON status GET "${summary}" status)
string(JSON count LENGTH "${summary}" cases)
string(JSON marker GET "${manifest}" metadata migration_test)
string(JSON args GET "${manifest}" cases external-handles-1 args)
if(NOT status STREQUAL "passed" OR NOT count EQUAL 3 OR NOT marker STREQUAL "native"
    OR NOT args MATCHES "--workers")
  message(FATAL_ERROR "Campaign summary/provenance is incomplete")
endif()
file(STRINGS "${HARNESS_OUTPUT}/runs.jsonl" runs)
list(LENGTH runs run_count)
if(NOT run_count EQUAL 6)
  message(FATAL_ERROR "Expected two measured runs for each of three cases")
endif()
foreach(row IN LISTS runs)
  string(JSON warmup GET "${row}" warmup)
  string(JSON status GET "${row}" output status)
  string(JSON run_manifest_sha256 GET "${row}" manifest_sha256)
  if(warmup OR NOT status STREQUAL "ok" OR NOT run_manifest_sha256 STREQUAL manifest_sha256)
    message(FATAL_ERROR "Unexpected campaign run: ${row}")
  endif()
endforeach()

# JSON serializers may emit valid scientific notation for small benchmark
# metrics. Exercise both exponent signs through the same CMake harness path.
set(numeric_output "${HARNESS_OUTPUT}/scientific-metric")
set(numeric_cases "${numeric_output}-cases.json")
set(numeric_config "${numeric_output}.cmake")
file(WRITE "${numeric_cases}" [==[
{"cases":[
  {"name":"negative-exponent","args":["-E","echo","{\"mean_s\":3.7663333333333336e-05}"]},
  {"name":"positive-exponent","args":["-E","echo","{\"mean_s\":1.25e+01}"]}
]}
]==])
file(WRITE "${numeric_config}"
  "set(HARNESS_NAME scientific-metric)\n"
  "set(HARNESS_BINARY [==[${CMAKE_COMMAND}]==])\n"
  "set(HARNESS_OUTPUT [==[${numeric_output}]==])\n"
  "set(HARNESS_CASES [==[${numeric_cases}]==])\n"
  "set(HARNESS_ROUNDS 1)\n"
  "set(HARNESS_WARMUP 0)\n"
  "set(HARNESS_TIMEOUT 10)\n"
  "set(HARNESS_PARSER json)\n"
  "set(HARNESS_METRICS mean_s)\n"
  "set(HARNESS_INVARIANTS )\n"
  "set(HARNESS_PERF_EVENTS )\n"
  "set(HARNESS_WORKING_DIRECTORY [==[${HARNESS_OUTPUT}]==])\n"
  "set(HARNESS_AFFINITY none)\n"
  "set(HARNESS_CPU_COUNT 0)\n"
  "set(HARNESS_RANDOMIZE FALSE)\n"
  "set(HARNESS_SEED 1)\n"
  "set(HARNESS_EXECUTION_BLOCKED FALSE)\n"
  "set(HARNESS_COMPILER regression)\n"
  "set(HARNESS_GENERATOR regression)\n"
  "set(HARNESS_PROJECT regression)\n")
file(MAKE_DIRECTORY "${HARNESS_OUTPUT}")
execute_process(COMMAND "${CMAKE_COMMAND}" "-DHARNESS_CONFIG=${numeric_config}"
  -P "${SOURCE_DIR}/cmake/dagflow/benchmark/HarnessCMake.cmake"
  RESULT_VARIABLE numeric_rc OUTPUT_VARIABLE numeric_out ERROR_VARIABLE numeric_err TIMEOUT 30)
if(NOT "${numeric_rc}" STREQUAL "0")
  message(FATAL_ERROR "Scientific metric harness failed: ${numeric_out}\n${numeric_err}")
endif()
file(READ "${numeric_output}/summary.json" numeric_summary)
string(JSON negative_micro GET "${numeric_summary}" cases negative-exponent metrics mean_s median)
string(JSON positive_micro GET "${numeric_summary}" cases positive-exponent metrics mean_s median)
if(NOT negative_micro EQUAL 37 OR NOT positive_micro EQUAL 12500000)
  message(FATAL_ERROR "Scientific metrics were scaled incorrectly: ${negative_micro};${positive_micro}")
endif()

set(base "${HARNESS_OUTPUT}/summary.json")
function(compare candidate expected)
  execute_process(COMMAND "${CMAKE_COMMAND}" "-DBASELINE=${base}"
    "-DCANDIDATE=${candidate}" -DMAX_REGRESSION_PERCENT=0
    -P "${SOURCE_DIR}/cmake/CompareDagFlow.cmake"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
  if(expected STREQUAL "pass" AND NOT "${result}" STREQUAL "0")
    message(FATAL_ERROR "Identical campaigns rejected: ${output}\n${error}")
  elseif(expected STREQUAL "fail" AND "${result}" STREQUAL "0")
    message(FATAL_ERROR "Invalid campaign comparison accepted")
  endif()
endfunction()
compare("${base}" pass)
set(candidate "${HARNESS_OUTPUT}/candidate")
file(MAKE_DIRECTORY "${candidate}")
file(WRITE "${candidate}/manifest.json" "${manifest}")
string(JSON value GET "${summary}" cases external-handles-1 metrics run_p50_us median)
math(EXPR slower "${value}*2")
string(JSON modified SET "${summary}" cases external-handles-1 metrics run_p50_us median "${slower}")
file(WRITE "${candidate}/summary.json" "${modified}")
compare("${candidate}/summary.json" fail)
file(WRITE "${candidate}/summary.json" "${summary}")
string(JSON modified SET "${manifest}" cases external-handles-1 args 3 "\"99\"")
file(WRITE "${candidate}/manifest.json" "${modified}")
compare("${candidate}/summary.json" fail)
string(JSON modified SET "${summary}" status "\"failed\"")
file(WRITE "${candidate}/summary.json" "${modified}")
compare("${candidate}/summary.json" fail)
message(STATUS "Native campaign, provenance and comparison checks passed")
