# Fault-injection coverage for exceptional runtime paths

The baseline LCOV artifact dated 2026-10-21 05:36 reported 1,777/1,799
instrumented lines (98.8%), with 22 old gaps remaining. Those gaps cannot
be exercised by feasible, defined public API traces; see
[`coverage-gaps-2026-10-21.md`](coverage-gaps-2026-10-21.md).

This addition builds **a separate static copy of the runtime** named
`dagflow_test_fault_runtime`. Its `DAGFLOW_TEST_FAULT_INJECTION` flag is
private, and the *normal* `DagFlow_static` / `DagFlow_shared` runtime retains
its previous code paths and public API. The fault PBT target is linked only to
that instrumented copy. Faults are thread-local and reset after each generated
command, so independent worker threads are never globally poisoned.

| Original coverage gap | Deterministic property / transition |
| --- | --- |
| `include/dagflow/detail/idle_accounting.hpp:54`, `src/idle_accounting.cpp:39-40` | Saturate worker and producer publication checks without writing overflowing counters. Failed attempts leave obligations unchanged; subsequent publishes/retirements still work. |
| `src/idle_accounting.cpp:111-114` | Inject a wait error while a real publication is outstanding, assert cleanup, retire the obligation, then successfully wait again. |
| `src/parking_lot.cpp:103-107` | Inject a wait error after announcing/sleeping, then prove the idle bitmap was cleared and subsequent registration/wake preserves epoch semantics. |
| `include/dagflow/task_graph.hpp:165`, `src/task_graph.cpp:15` | Lower the node/edge capacity threshold only in the test copy, assert rejection is transactional, and execute the previously admitted graph. |
| `src/task_graph.cpp:65-66` | Bypass the *earlier* validation in the test copy to exercise independent validation in `seal()`. A malformed graph is not executed. |
| `src/task_scope.cpp:63`, `src/thread_pool.cpp:192` | Simulate saturated publisher reservations while leaving actual counters intact; assert exception and successful subsequent publication. |
| `src/thread_pool.cpp:329,332-334` | Inject failure at each accepted-prefix length in batch publication. Verify exact execution count and weak capture owners expired after `wait_idle()`. |

Additionally, the `IdleAccounting::retire()` fail-fast saturation path is
checked using an isolated Linux child process. This does not corrupt private
counter storage or require data races/undefined behavior; the death-contract
check is skipped on non-Linux and TSan builds.

## Running

```sh
cmake --preset coverage
cmake --build --preset coverage --target dagflow_fault_injection_property_tests
ctest --preset coverage -R '^dagflow_fault_injection_property_tests$' --output-on-failure
# Full report, if lcov + genhtml are installed:
cmake --build --preset coverage --target coverage-report --parallel 4
```

A failed generated trace prints a seed and a minimal replay using
`DAGFLOW_PROPERTY_SEED`, `DAGFLOW_PROPERTY_MODEL`, and `DAGFLOW_PROPERTY_TRACE`.
To increase randomized transitions:

```sh
DAGFLOW_PROPERTY_SEED=42 DAGFLOW_PROPERTY_CASES=96 DAGFLOW_PROPERTY_STEPS=128 \
  out/build/coverage/dagflow_fault_injection_property_tests
```

**Interpretation:** Instrumented fault-path line coverage is not a proof that
physical 64-bit saturation or a real OS condition-variable failure occurred.
The test verifies branch behavior and recovery *under injected preconditions*.
Concurrency/liveness correctness still depends on sanitizer, adversarial race,
and stress tests. New injection checks can add their own counted source lines,
so the post-patch global LCOV percentage must be measured, not extrapolated.
