# Bounded runtime schedule exploration

These tests execute the actual C++ runtime and systematically replay runnable
actor choices at explicit checkpoints. They do not substitute an abstract
protocol implementation or random yields for runtime execution.

```sh
cmake --preset core -B out/build/protocol-exploration
cmake --build out/build/protocol-exploration \
  --target dagflow_schedule_exploration_tests --parallel 2
ctest --test-dir out/build/protocol-exploration \
  -L schedule-exploration --output-on-failure --no-tests=error
```

## Bounds and independent oracles

| Suite | Actors / fixture | Histories | Contract |
| --- | --- | ---: | --- |
| `deque-owner-thief` | Owner/thief, one item, two slots | 13 | Unique last-item custody; failed output unchanged |
| `deque-two-thieves` | Two thieves, one item | 20 | Stale snapshots cannot grant duplicate custody |
| `deque-publication` | Publisher/thief, one item | 6 | Slot writes do not themselves publish custody; eventual exact acquisition |
| `scheduler-transfer` | Two owners, two local packets, batch stealing | 20 | Every packet acquired once; stolen siblings remain visible |
| `completion-register-retire` | Two credit owners and dependent registrar | 504 | Credit conservation, cleanup before ready, first error, early/late joins and unrelated source retaining its credit |
| `scope-cancel-execute-complete` | One worker/canceller, one accepted task | 6 | Cancellation observation determines invocation; both outcomes clean up before completion |
| `graph-cancel-propagation` | One worker/canceller, two-node chain | 4 | Root/successor suppression, predecessor effects, dependency/inline-continuation cancellation and credit drainage |
| `announce-park-wake` | One waiter/publisher, one publication | 16 | Final scan/epoch handshake, announced waiter progress and sleeping-state retirement |

Total: **589 runtime histories**. The enumerator self-test checks the complete,
independently listed set of six merges of two two-step programs, with no repeated
history. Counts include choices with equivalent outcomes; this is not
partial-order reduction.

Queue checkpoints split slot publication, owner bottom update, thief snapshot
and competing CAS operations. Completion checkpoints split registration,
payload cleanup, ready publication and gate closure. Scope checkpoints are on
both sides of the cancellation load and callable destruction; graph checkpoints
are inside dependency propagation and inline continuation. Macros disappear
outside `DAGFLOW_FUZZ_HOOKS`; ordinary libraries contain no controller locks.

## Exploration and replay

[`schedule_exploration_support.hpp`](schedule_exploration_support.hpp) starts
each history in fresh storage. One actor runs to its next checkpoint or exit;
every alternative runnable actor creates a prefix to explore later. Thread
program order is preserved. Limits are 64 choices per history and 20,000
histories per suite; exceeding either fails rather than silently truncating.

Failures print the suite, actor-choice trace and invariant. Replay selects one
suite and a decision prefix; after the prefix, the lowest runnable actor
deterministically completes the history:

```sh
DAGFLOW_SCHEDULE_SUITE=deque-owner-thief \
DAGFLOW_SCHEDULE_TRACE=0,0,0,1,1 \
  out/build/protocol-exploration/dagflow_scheduler_schedule_exploration_tests
```

A missing suite or unavailable actor is rejected. A five-second controller
watchdog prevents hanging teardown. Assertion exit 86 and watchdog exit 87
are distinct; the mutation runner does not count watchdogs as assertion kills.

## Limits

These are **SC executions at declared checkpoints**. Controller synchronization
orders actors more strongly than an unconstrained runtime; this cannot establish
acquire/release correctness, compiler reorderings or C++ weak-memory behavior.
The relaxed-publication mutant consequently survives.

Parking has a terminal blocking tail: after a false CV predicate, the publisher
must continue to satisfy the wait. The real runtime mutex/CV handshake executes
there; OS scheduling and spurious wakeups inside the tail are not enumerated.
`wait_for(predicate)` rechecks the epoch at its deadline, so missing notify can
survive while delaying progress until that deadline. We do not turn host
scheduling latency into a supposedly deterministic correctness assertion.

Scheduler transfer and Pool epilogues are separate fixtures, not one exhaustive
model of every queue, packet and accounting instruction. Arbitrary graph sizes,
multiple graph lanes, nested helping and every lock acquisition order remain
outside these bounds. Existing adversarial tests, PBT and sanitizers still
apply. The graph runtime has per-node atomics; this adds no graph-wide lock.

## Validation

The core-derived build passed **87 tests**. Four new executables passed a
separate Clang Debug ASan/UBSan build; `ASAN_OPTIONS=detect_leaks=0` was necessary
because LeakSanitizer process inspection is restricted in this environment.
The [mutation campaign](protocol-mutation-campaign.md) detected 14 of 16 chosen
violations and explicitly reports the two survivors above.
