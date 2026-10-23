# Independent reference models, concurrent histories, transition coverage and mutations

This stage extends the existing stateful PBT infrastructure **without removing
unit tests or editing the production runtime**. Tested base: `DagFlow (1).zip`
with `../.cache/DagFlow-next-reachable-branch-PBT.patch` applied. All new tests use
`DAGFLOW_PROPERTY_SEED`, `DAGFLOW_PROPERTY_CASES`, `DAGFLOW_PROPERTY_STEPS`,
`DAGFLOW_PROPERTY_MODEL` and `DAGFLOW_PROPERTY_TRACE` from
`tests/property_support.hpp`, including valid-trace shrinking.

## Independent TaskGraph reference model

Target: `dagflow_reference_graph_model_property_tests`.

* Generator: acyclic 1–8-node graphs, random forward/duplicate edges,
  0/1/2/3/7/65/129 requested tokens, Drop/Block policies, capacities, worker
  concurrency, affinity and priority. Error and cancel histories are separate,
  deterministic configurations.
* Oracle: plain, sequential reachability transitive closure over an adjacency
  matrix; for each node expected calls are `max(1,tokens)` for Block and
  `min(max(1,tokens),capacity)` for Drop. It shares no CSR, credit, scheduling,
  lane or publication implementation with DagFlow.
* Checks: all expected calls execute, no duplicates, every transitive
  predecessor finished before a successor's callback starts, completed handles
  survive reset/replay and clear. Controlled overflow-Fail must report an
  error without executing the rejected callback; cancellation of a gated root
  suppresses its unstarted successor.
* Transition counters: Block, Drop, Drop-to-zero, fan-in, fan-out, replay,
  failure, cancel and clear; printed with `ctest -V`. Counts are **observed**
  coverage, not a proof that all valid state combinations were explored. Small
  selected/replay runs intentionally do not need to hit every category.

## Pool reference model

Target: `dagflow_pool_reference_property_tests`.

* Model is a set of accepted effect obligations and completion observations,
  not a clone of work-stealing internals. For each accepted packet, observable
  effect count must equal one after completion/idle drain and may never exceed
  one before drain.
* Histories cover handled task success/failure, detached success, fan-in of
  handles, individual wait, idle drain, admission after close, child submitted
  and waited by a worker, and recreation with historical handles surviving.
* Important distinction: a closed pool rejects **external** admission, but
  a running worker can still submit descendants. `combine` completion itself
  does not execute user work.
* Covers application-visible semantics. Does not claim complete worker/queue
  state, fairness, arbitrary concurrent shutdown, or linearizability of Pool.

## Concurrent real-time histories (success-side linearization)

Target: `dagflow_concurrent_history_property_tests`.

* 2 threads, each performs three `try_push`/`try_pop` calls against a capacity-2
  `ring_mpmc<int,2>`. Optional initial occupancy 0, 1 or 2. No probes of
  private queue internals.
* Every call records invocation and response sequence numbers from a single
  seq-cst event counter. The checker enumerates sequential orders respecting
  `response(A) < invocation(B)` and checks **successful** operations (FIFO,
  capacity, successful admission), failed output unchanged and the final drain.
  The queue explicitly allows transient unsuccessful probes of busy slots:
  such failures are allowed when calls overlap, but not when quiescent.
* Backtracking explores no more than `6!` orders per generated history.
* This is a *success-side* FIFO history checker with conservative treatment
  of false-full/false-empty under contention, not a proof of full linearizability
  of the unsuccessful status. It does not model individual reservation steps.
* Output includes outcome counts and observed overlap counts. Further
  adversarial fuzzing/sanitizer checks remain necessary for weak-memory
  concurrency bugs and long histories.

## Mutation verification

A **real compile-and-run mutation campaign** is provided, separately from
ordinary CTest. It writes four one-line mutants of
`include/dagflow/detail/ring_mpmc.hpp` to temporary directories and compiles
`concurrent_history_property_tests.cpp` against each mutated implementation.
Original working-tree code is never modified. Mutants are:

1. Discard published payload.
2. Discard acquired payload.
3. Return successful admission when full.
4. Return failed acquisition after consuming a slot.

The campaign first tests the original implementation. It reports assertion
kills, timeouts and survivors separately; it returns nonzero for a surviving
mutant or timeout. This score is **only over four selected queue mutants**,
not an overall DagFlow mutation score. Purely synthetic incorrect observations
in each model are also used to calibrate its oracle, but are not included in
that score.

```sh
cmake --build --preset core --target dagflow_model_property_tests -j 4
ctest --preset core -L pbt --output-on-failure

# Print transition counts for these particular models:
ctest --test-dir out/build/core -V \
  -R 'dagflow_(reference_graph_model|pool_reference|concurrent_history)_property_tests'

# Direct, independent mutation campaign (requires Python3 and a C++23 compiler):
python3 scripts/mutation_campaign.py --cases 32 --steps 36

# Or the optional CMake target, not added to normal test runs:
cmake --build --preset core --target dagflow_queue_mutation_campaign
```

## Limitations and next verification work

This work establishes concrete reference oracles and reproducible concurrent
histories; it does **not** formally prove memory safety, custody, freedom from
all races, liveness, fairness or linearizability of the entire DagFlow runtime.
The TaskGraph oracle covers successful DAGs with Block/Drop and constrained
failure/cancellation schedules. General concurrent cancellation during
propagation remains adversarial-checkpoint territory. The Pool model is an
observable-effect oracle, not a complete linearizable scheduler specification.
For future work, expand the mutation matrix to scheduler/completion state
transitions and add deterministic callback scheduling for graph concurrency
histories and longer real-time partial orders.
