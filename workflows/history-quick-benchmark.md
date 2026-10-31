# Quick history comparison demo

`history-quick-benchmark.json` runs the existing public API `chain` benchmark ten
times in a configured `runtime-campaign` build tree. The two runs must use the
same manifest and arguments: CMakeWorkflow rejects workload changes as
incompatible for statistical comparison.

From the DagFlow root, with CMakeWorkflow on `PATH`:

```sh
cmake --preset runtime-campaign
cmake --build out/build/runtime-campaign --target dagflow_public_api_bench
CMakeWorkflow workflow run workflows/history-quick-benchmark.json \
  --history-db out/workflow-history-demo.sqlite
CMakeWorkflow workflow run workflows/history-quick-benchmark.json \
  --history-db out/workflow-history-demo.sqlite
CMakeWorkflow history runs --limit 5 --db out/workflow-history-demo.sqlite
```

In the VS Code extension, set `cmakeWorkflow.historyDb` to
`out/workflow-history-demo.sqlite`, refresh **History & Comparison**, and choose
the earlier run as Baseline and the later run as Candidate. **Compare Runs**
shows the two sample groups; **Detect Regression** can test
`process_wall_time`. Each run contributes ten process timing samples. This is a
quick UI and protocol demonstration, not a controlled performance study:
process startup and background load can dominate such short measurements.
