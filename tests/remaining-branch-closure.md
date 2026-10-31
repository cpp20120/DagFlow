# Remaining reachable-branch PBT (2026-10-21)

Input: the **99.8% lines / 90.1% branches** LCOV report, measured with
`genhtml --filter exception`. This report was generated **before** this patch.

## Properties and deterministic boundaries

New target: `dagflow_branch_gap_fault_property_tests`. It links **only** the
isolated `dagflow_test_fault_runtime`, not the production DagFlow target. The
single extra internal test control `reject_external_ingress` simulates a
bounded series of full-ingress results **before custody is transferred**. It
is thread-local, leaves queue internals untouched, and is compiled out of
ordinary builds. It does not simulate a scheduler race; the separate
adversarial suite remains responsible for concurrency interleavings.

- Single external submission and batches of 1..8 and 65 callables, high/normal
  priority and optional affinity: force 0..6 failed ingress attempts, then
  verify the exact retry count is consumed, exactly-once execution, an idle
  barrier, and recovery with a healthy follow-up submission.
- Allocation at the runtime boundary: fail exactly the first allocation or
  the allocation following a bounded successful prefix, check `bad_alloc`,
  construction/destruction balance and subsequent successful ownership.
  Check zero-size allocations, null deleters, STL allocation count overflow
  and `make_owned_array` size rejection without allocating huge buffers.
- TaskGraph: reject invalid priority/overflow in `seal`, reject
  `Overflow::Block` with `capacity=0` at builder admission; check recovery,
  zero tokens normalization, and cancellation with no active run.
- `scheduler_targeted_paths_test` additionally checks repeated acquisition
  of completely empty ingress at and beyond the 32nd periodic poll.
- TaskGraph with a zero-worker pool configuration checks the documented
  normalization to one worker, the resulting public worker-count API, and
  successful run/reuse.

### Targeted counters

Local GCC 14 instrumented **targeted** execution reached the four previously
uncovered executable lines in the old report:

- `src/runtime_memory.cpp:18-19`: first allocation rejected by hook
- `src/thread_pool.cpp:296`: normal external retry
- `src/thread_pool.cpp:330`: batch external retry

These are GCOV observations on the test-only runtime. They do **not** establish
new global LCOV branch percentages; only a complete `coverage-report` can
measure aggregate changes across production and fault-runtime objects.

### Test commands

```sh
cmake --preset coverage
cmake --build --preset coverage --target \
  dagflow_model_property_tests dagflow_scheduler_targeted_paths_test --parallel 4
ctest --test-dir out/build/coverage -L pbt --output-on-failure
ctest --test-dir out/build/coverage -R scheduler_targeted_paths --output-on-failure
cmake --build --preset coverage --target coverage-report --parallel 4
```

Use the repository's existing LCOV `genhtml` consistency workaround as needed;
it is independent of these tests. To rerun a failure, set
`DAGFLOW_PROPERTY_SEED`, `DAGFLOW_PROPERTY_TRACE`, and
`DAGFLOW_PROPERTY_MODEL` as printed by the PBT runner.

## Why the remaining percentage need not be 100%

The previous per-line branch inventory includes deliberately unexecutable
outcomes under valid invariants: scheduler batch `try_push()` triggering
`std::terminate`, invalid completion-credit assertions, `uint64_t` identity
wraparound, a `small_vector` already containing `max_size()` elements, and a
positive range divided into zero nonempty chunks. GCC also counts duplicated
template instantiations and generated control flow, including fallthroughs
remaining after `exception` filtering. These do not become genuine reachable
protocol transitions by increasing random case counts. Do **not** corrupt
internal state or allocate exabytes to force a cosmetic 100% branch metric.
