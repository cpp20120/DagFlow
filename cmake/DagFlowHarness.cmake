if(NOT TARGET dagflow_runtime_suite)
  message(FATAL_ERROR "DAGFLOW_BUILD_HARNESS requires DAGFLOW_BUILD_RUNTIME_SUITE=ON")
endif()
dagflow_add_harness(dagflow_runtime_campaign
  TARGET dagflow_runtime_suite CASES "${PROJECT_SOURCE_DIR}/bench/runtime_cases.json"
  ROUNDS 3 WARMUP_RUNS 1 TIMEOUT 60 RANDOMIZE
  METRICS run_p50_us payload_tasks_per_second INVARIANTS checksum
  METADATA "allocator=${DAGFLOW_ALLOCATOR}")
