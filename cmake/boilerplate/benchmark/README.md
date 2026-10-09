# General process harness

`HarnessCMake.cmake` is the self-contained process harness for native targets. CMake owns the build graph, repeated process execution, CPU placement, provenance, JSON result parsing, fixed-point metric statistics and optional Linux `perf stat` invocation.

The harness has no Python runtime dependency. `boilerplate_add_harness()` generates a per-target config and executes `HarnessCMake.cmake` through `cmake -P`.

A cases file can be a JSON array for the compact form, or an object with shared `defaults`, named `groups`, and `cases`. Each case may define `name`, `args`, `env`, `cwd`, `cpu_count`, an exact `cpus` list, and free-form `metadata`. Object values in `env` and `metadata` are merged with defaults; case values win:

```json
{
  "defaults": {
    "args": ["--scenario", "chain"],
    "env": {"ALLOCATOR": "system"},
    "metadata": {"suite": "scaling"}
  },
  "groups": {
    "smoke": ["chain-1"],
    "full": ["chain-1", "chain-4"]
  },
  "cases": [
    {"name":"chain-1", "args":["--workers","1"], "cpu_count":1},
    {"name":"chain-4", "args":["--workers","4"], "cpu_count":4}
  ]
}
```

The executable can emit one JSON object line per invocation. With the default `json-line` parser, a `status` field is checked when present (`"ok"` is required). `METRICS` selects numeric fields to summarize and `INVARIANTS` selects fields that must remain identical across measured runs.

```cmake
boilerplate_add_harness(runtime_matrix
  TARGET runtime_bench
  CASES "${CMAKE_CURRENT_SOURCE_DIR}/bench/cases.json"
  ROUNDS 7 WARMUP_RUNS 2 TIMEOUT 120
  AFFINITY physical RANDOMIZE
  METRICS run_p50_us payload_tasks_per_second
  INVARIANTS checksum)
```

Add `CASE_GROUP smoke` to that declaration to run only the named subset. The
same file can then serve quick checks and full scaling runs without multiplying
CMake targets or presets.

Build normally, then run explicitly:

```sh
cmake --build out --target run_runtime_matrix
```

The output directory contains `manifest.json`, `runs.jsonl`, `summary.json`, and per-process stdout/stderr logs. Metric summaries use fixed-point micro-units (`scale: 1000000`) so CMake can aggregate decimal values without a floating-point runtime. Linux counters can be requested with `PERF_EVENTS`; raw `perf stat` CSV is retained beside each run log.

## What belongs here vs. in a project adapter

The generic harness owns process lifecycle, timeout propagation, affinity, provenance, raw logs, JSON parsing, fixed-point statistics and perf invocation. Project adapters should keep scenario semantics, matrix generation, expected checksums, allocator/runtime-specific build definitions, comparison rules and domain reports. That keeps a DagFlow benchmark, an HTTP server benchmark and a Qt/graphics smoke workload on the same runner without teaching the runner what a shard, request, graph or frame is.
