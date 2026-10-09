# Scope admission, completion and park/wake hot paths

> **Historical workflow note (October 2026).** This report preserves its original
> benchmark commands and measurements. The former `scripts/benchmark_*.py`
> orchestration is retired; those commands are not part of the current build.
> For reproducible runs use [CMake benchmark campaigns](../benchmarks/campaigns.md).


This change follows the observations in
[the complete-run analysis](../benchmarks/complete-run-analysis.md).
No performance measurements were run for this change; the existing results
describe the preceding implementation.

## Changes

- `TaskScope` admission uses one atomic word for closed/cancelled flags and
  in-flight external reservations. A reservation protects the root credit while
  it is forked. Closing retires the root immediately if there are no reservations;
  otherwise the last publisher retires it. Child admission forks the live parent
  credit after checking cancellation, without reserving the root.
- Scope error publication has a separate mutex. Ordinary admission and successful
  credit retirement do not acquire the scope or completion mutex. This does not
  imply that allocators, queue overflow, or sleeping-worker notification are
  lock-free.
- Completion error lookup has an atomic no-error fast path. A separate dependent
  gate arbitrates registration against terminal completion; only registered
  dependency lists need the completion mutex. External completion waits use
  atomic wait/notify. The standard library may use internal locks.
- Registered idle workers pause for up to 128 epoch checks before entering the
  condition-variable wait. Notifiers skip the waiter mutex when the worker has
  not announced sleeping. Sequentially consistent sleeping/epoch operations
  preserve the predicate/notification handshake. The final queue scan and worker
  recruitment relay remain in place.
- Empty Chase-Lev steal probes return before the sequentially consistent fence.
  A potentially successful probe still uses the existing fence, bottom recheck
  and last-item arbitration.

The spin bound is an initial choice, not a measured optimum. Spinning can spend
more idle CPU; the steal precheck adds a load to successful probes. Compare these
costs under the same workload and CPU placement before claiming a speedup.

Protocol details: [scope lifetime](../task-scope-lifetime.md),
[completion ownership](../explicit-ownership.md), and
[scheduler parking](../scheduler-topology.md).

## Correctness validation

| Build | Checks | Result |
| --- | --- | --- |
| Release, Clang, system allocator | Full CTest suite | 23/23 passed |
| Debug, ASan + UBSan, system allocator | Queue, idle accounting, topology, pool queue/lifecycle, batch, ownership, scope, graph, fairness | 10/10 passed |
| Debug, TSan, system allocator | Same targeted suite | 10/10 passed |

The final error-publication adjustment was followed by another full Release run
and ownership/scope reruns under both sanitizer builds; all passed. LeakSanitizer
could not run in the sandbox because of its ptrace restriction, so the successful
ASan/UBSan runs used `detect_leaks=0`.

Added regressions cover simultaneous publish/close/cancel, children reserved
across close/cancel, completion while its cold mutex is held, dependency
registration racing completion (including error propagation), deep iterative
dependency retirement, and parking publication races through both ingress and
local stealing.

Build directories are `out/build/runtime-paths-check`,
`out/build/runtime-paths-asan`, and `out/build/runtime-paths-tsan`.

For a new measurement run, executed by the user:

```sh
python3 scripts/benchmark_all.py --output out/benchmarks/park-scope-after
```

Keep the preceding `out/benchmarks/complete-run` directory for comparison.
