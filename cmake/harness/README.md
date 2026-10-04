# General process harness

`run.py` is an optional, dependency-free Python harness for native targets. CMake remains responsible for the build graph; Python owns repeated process execution, CPU placement, command/provenance logs, JSON result parsing, basic statistics and optional Linux `perf stat` counters.

Nothing imports Python merely by including `DagFlow.cmake`. Python is discovered only when `dagflow_add_harness()` is used.

A cases file is a JSON array. Each case may define `name`, `args`, `env`, `cwd`, `cpu_count`, an exact `cpus` list, and free-form `metadata`:

```json
[
  {"name":"chain-1", "args":["--scenario","chain","--workers","1"], "cpu_count":1},
  {"name":"chain-4", "args":["--scenario","chain","--workers","4"], "cpu_count":4}
]
```

The executable can emit one JSON object line per invocation. With the default `json-line` parser, a `status` field is checked when present (`"ok"` is required). `METRICS` selects numeric fields to summarize and `INVARIANTS` selects fields that must remain identical across measured runs.

```cmake
dagflow_add_harness(runtime_matrix
  TARGET runtime_bench
  CASES "${CMAKE_CURRENT_SOURCE_DIR}/bench/cases.json"
  ROUNDS 7 WARMUP_RUNS 2 TIMEOUT 120
  AFFINITY physical RANDOMIZE
  METRICS run_p50_us payload_tasks_per_second
  INVARIANTS checksum)
```

Build normally, then run explicitly:

```sh
cmake --build out --target run_runtime_matrix
```

The output directory contains `manifest.json`, `commands.json`, `runs.jsonl`, `summary.json`, and per-process stdout/stderr logs. Linux counters can be requested with `PERF_EVENTS`; they are collected through `perf stat` and never silently mixed with plain timing runs unless explicitly requested.

## What belongs here vs. in a project adapter

The generic harness owns process lifecycle, timeout/kill, affinity, provenance, raw logs, JSON parsing, statistics and perf parsing. Project adapters should keep scenario semantics, matrix generation, expected checksums, allocator/runtime-specific build definitions, comparison rules and domain reports. That keeps a DagFlow benchmark, an HTTP server benchmark and a Qt/graphics smoke workload on the same runner without teaching the runner what a shard, request, graph or frame is.
