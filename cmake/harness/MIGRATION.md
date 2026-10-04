# Extraction from the DagFlow research scripts

The supplied DagFlow scripts contain two different layers. The reusable layer is now in `dagflow_harness`; the experiment adapters should remain in the project that owns the executable and its result schema.

## Moved to the generic harness

Common code repeated across `benchmark_main.py`, `benchmark_suite.py`, `benchmark_public_api.py`, `benchmar_tbb.py`, `benchmark_github.py`, `benchmark_graph_execution.py`, `benchmark_drain_wake.py`, `recheck_graph_execution.py`, and the profiling scripts now has a home:

- process-group lifecycle, timeout and failure propagation: `CommandRunner`;
- stdout/stderr + command logs: `CommandRunner`;
- SHA-256 provenance and deterministic source snapshot hashes: `digest`, `tree_digests`;
- Linux CPU topology / physical-core representatives / affinity: `physical_cpus`;
- `perf stat -x ';'` parsing with unavailable events distinct from real zeroes: `parse_perf_stat`;
- JSON manifests and atomic-ish replacement: `save_json`;
- randomized repeated case execution: declarative `run.py run`;
- numeric metric summaries and invariant checking: declarative runner;
- randomized A/B order and generic paired statistics: `randomized_paired_jobs`, `paired_summary`;
- exploratory bootstrap CI for paired medians: `bootstrap_median_ci`.

`CommandRunner.command()` intentionally exists for gradual migration: old experiment scripts can keep their own parsing/report generation while deleting their duplicate subprocess/timeout/logging implementation.

## Keep in DagFlow/WebServer adapters

These are semantics, not harness plumbing, and should not move into the generic package:

- the `DAGFLOW_*` CMake cache API and historical `TP_*` translation in `benchmark_build.py`;
- scenario names and legal parameter validation (`shards`, `producers`, `submit_batch`, graph shapes, request modes, etc.);
- how an executable proves correctness (`checksum`, `duration_satisfied`, exact graph results);
- normalization such as cycles/logical-task or allocation requests/run;
- allocator source rewriting and frozen-source comparisons;
- oneTBB-specific compilation and DagFlow-vs-oneTBB API mapping;
- benchmark-specific diagnostics keys and reports;
- source/binary selection rules for regression rechecks.

Those adapters can import the generic primitives or generate a cases JSON and use the declarative runner.

## Intentionally not generalized yet

Two mechanisms in the research scripts are useful but have stronger executable/tool contracts and should become optional extensions rather than infecting the base runner:

1. **phase-gated perf** (`perf --control=fd` plus benchmark-owned `--perf-control-fd/--perf-ack-fd`); the executable must explicitly implement the handshake;
2. **flamegraph/profile campaigns** (`perf record/report/script`, DWARF vs frame-pointer stacks, custom flamegraph rendering).

They should live as `perf` extensions on top of `CommandRunner`, not in CMake and not in the minimal execution path.
