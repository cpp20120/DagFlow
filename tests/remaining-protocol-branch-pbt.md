# Reachable protocol branches: additional stateful PBT

Base: the user's `DagFlow (1).zip` archive (2026-10-21). Additive only: no
production source changes, no removal or replacement of existing unit tests.

## New CTest targets

- `dagflow_remaining_protocol_property_tests` (`pbt;stateful`):
  - **Queued scope custody:** a single worker is held by an explicit atomic
    gate while 0/1/2/17/65 child callbacks are published with differing
    priorities and submission modes. `close()` drains every callback; a
    subsequent `cancel()` instead prevents every pending callback from
    entering the user function. Captures are destroyed, completion becomes
    ready, no error is recorded, and late external admission is rejected.
  - **GraphScope chunking:** 0/1/63/64/16384/16385/32769 elements,
    iterator and borrowed-span overloads, direct and `parallel_for_after`,
    destructor autorun, explicit run/wait and clearing an unstarted graph.
    A fresh untouched scope additionally checks its inert destructor.
  - **Forward-range partitioning:** `std::forward_list` iterators and lvalue
    range overloads with boundary lengths, and `Spawn`/`Enqueue` publication.
    Each element is visited exactly once with no overlap or omission.
  - Each model uses the existing seed/trace/replay/shrinker infrastructure.
- `dagflow_deque_last_slot_race_test` (`adversarial;concurrency`):
  - The owner first pops from a full two-slot deque to cover `t != b`.
  - For 12,000 generations, one persistent thief and the owner race on the
    final item behind two `std::barrier` rendezvous points.
  - Exactly one side receives the item; the loser retains its sentinel
    output; each generation leaves the queue empty.

## Local verification

- GCC 14 Debug: **20/20** property/PBT CTest targets passed; both new targets passed.
- GCC 14 `--coverage -O0 -g`: both CTest targets passed.
- Stateful property target: five additional seeds with `CASES=24`, `STEPS=36`.
- Deque race: 30 independent executions passed.
- Local gcov of instrumented test targets observed both outcomes of
  `chase_lev_deque.hpp`'s `t == b`, both outcomes of the scope cancellation
  check, and empty/nonempty GraphScope ranges. This is targeted execution,
  **not** an updated full-suite LCOV percentage.

## Reproduce

```sh
cmake --preset coverage
cmake --build --preset coverage --target \
  dagflow_remaining_protocol_property_tests \
  dagflow_deque_last_slot_race_test --parallel 4
ctest --test-dir out/build/coverage \
  -R 'dagflow_(remaining_protocol_property_tests|deque_last_slot_race_test)$' \
  --output-on-failure
# For a whole-suite coverage measurement (and branch filter), rerun the
# project's coverage-report target and regenerate html-noexcept separately.
```

The total branch percentage need not reach 100%. Some GCC paths are
compiler-generated; invariant-violation / overflow guards must not be triggered
by corrupting objects or performing unsafe allocations for a cosmetic metric.
