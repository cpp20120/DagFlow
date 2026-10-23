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
| `untimed-notification` | One waiter/publisher, one publication, untimed CV | 16 | A committed sleeper requires an actual notification; epoch changes alone cannot satisfy the oracle |
| `graph-lanes-fanin` | Two workers, four root tokens in two lanes, diamond graph | 96 | Final lane join precedes propagation; branches and fan-in execute once; clear releases captures |
| `graph-lanes-fanin-cancel` | Same graph plus external canceller | 570 | Cancelled lanes retire; cancellation cannot duplicate effects or release storage early |
| `fanin-cancel-complete` | Two in-flight scalar roots plus canceller, one shared successor | 176 | Both predecessor effects precede fan-in; cancellation suppresses unstarted successor; live invocations prevent readiness |
| `nested-help-shutdown` | One worker with parent/child/grandchild, external closer | 120 | Nested helping restores wait depth; close permits descendants; shutdown drains callable destruction before stopping workers |

Total: **1,567 runtime histories**. The enumerator self-test checks the complete,
independently listed set of six merges of two two-step programs, with no repeated
history. Counts include choices with equivalent outcomes; this is not
partial-order reduction.

Queue checkpoints split slot publication, owner bottom update, thief snapshot
and competing CAS operations. Completion checkpoints split registration,
payload cleanup, ready publication and gate closure. Scope checkpoints are on
both sides of the cancellation load and callable destruction; graph checkpoints
are inside dependency propagation, inline continuation and final lane join.
Shutdown exposes the boundary before physical drain. Macros disappear
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
The relaxed-publication mutant consequently survives this explorer. The separate
[weak-memory check](weak-memory.md) addresses publication with a source-level
argument and Relacy counterexamples; it does not make this SC explorer a
weak-memory checker.

Parking has a terminal blocking tail: after a false CV predicate, the publisher
must continue to satisfy the wait. The real runtime mutex/CV handshake executes
there; OS scheduling and spurious wakeups inside the tail are not enumerated.
The timed fixture checks the epoch handshake. Its `wait_for(predicate)` can
recheck a changed epoch at the deadline even if notify is missing. The separate
untimed fixture disables that fallback and records invocations inside the CV
wrapper's `notify_one()` itself. After its first false predicate the wrapper
also gates return on a new notification, so spurious wakes cannot clear the
sleeping flag and discharge the publisher's obligation before its check. An
initially true predicate retains the ordinary no-notification fast path.
When a waiter has reached a false predicate, the publisher must produce a
notification. Deleting that call therefore fails an assertion immediately,
including in the presence of spurious wakes.
This checks the notification obligation, not an OS wakeup latency threshold or
an exhaustive model of CV internals. The wrapper exists only in hook builds;
the test runtime exports the hook definition to keep consumer layouts uniform.

Scheduler transfer and Pool epilogues are separate fixtures, not one exhaustive
model of every queue, packet and accounting instruction. Arbitrary graph sizes,
arbitrary helping depth and every lock acquisition order remain outside these
bounds. In the two-lane fixtures only root lanes and cancellation are scheduled
actors: downstream packets and epilogues execute as a concurrent tail with
atomic invariants. The separate scalar fan-in fixture controls propagation and
completion checkpoints; both predecessors are already in flight before its
canceller starts. The nested-help fixture bounds depth at two waits and permits
the closer's physical drain to run as a blocking tail. Existing adversarial
tests, PBT and sanitizers still apply. The graph runtime has per-node atomics;
this adds no graph-wide lock.

## Validation

The core-derived build passed **88 tests**. All five exploration executables
passed a separate Clang Debug ASan/UBSan build; `ASAN_OPTIONS=detect_leaks=0` was
necessary because LeakSanitizer process inspection is restricted in this environment.
The [mutation campaign](protocol-mutation-campaign.md) detected 18 of 19 chosen
violations and explicitly reports the relaxed-publication survivor above.
