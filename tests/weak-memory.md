# Weak-memory verification

The campaign covers deque, parking, completion, idle accounting and shutdown. This page
documents the deque; [parking and completion](weak-memory-parking-completion.md)
and [idle accounting](weak-memory-idle-accounting.md), plus
[shutdown composition](weak-memory-shutdown.md), have separate
source mappings, ordering arguments, bounds and mutation results.

The deque check addresses the `deque-relax-publication` mutant that survives the
mutex-synchronized schedule explorer. It combines a source-level publication
argument with bounded executions under [Relacy](https://github.com/dvyukov/relacy).
It is not a proof of the entire deque or DagFlow runtime.

## Publication argument

Consider one owner pushing one item into a fresh deque and one thief attempting
to steal it. Storage outlives both threads. There is no pop, slot reuse, index
wraparound, or extra synchronization between producer and consumer.

In [`chase_lev_deque.hpp`](../include/dagflow/detail/chase_lev_deque.hpp), name
these operations:

| Event | Operation |
| --- | --- |
| P | Owner initializes the task payload before `try_push` |
| S | Owner stores the item into `buffer_[b & (N - 1)]`, relaxed |
| B | Owner stores `b + 1` into `bottom_`, release |
| L | Thief loads that published bottom value, acquire |
| R | Thief loads the slot, relaxed |
| C | Thief wins the `top_` CAS and returns the item |
| Q | Consumer reads the task payload after successful steal |

Within each thread, program order gives `P → S → B` and `L → R → C → Q`.
When L reads B, the release/acquire pair synchronizes. Thus `S happens-before R`
and `P happens-before Q`. These are the required visibility edges; the CAS
arbitrates custody, while B/L publish the data.
[C++ atomic ordering](https://eel.is/c++draft/atomics.order).

Atomic write-read coherence prevents R from reading a slot modification older
than S. There is no later slot modification in this fixture, so the successful
steal returns the published item. The non-atomic payload write P also precedes
Q through happens-before, making that payload access race-free.
[C++ happens-before and coherence](https://eel.is/c++draft/intro.races).

Changing only B to relaxed removes the synchronizes-with edge. The thief's
SC fence and SC CAS do not recreate it: the producer has no corresponding
release operation publishing these writes in this execution. Two failures are
then possible:

* The thief reads the new bottom but the old atomic slot value, wins CAS, and
  returns the wrong item. This is a stale atomic value counterexample.
* The thief reads the correct slot and wins CAS, but its subsequent non-atomic
  payload access has no happens-before relationship with initialization. This
  is a data race; a particular payload value is not a defined C++ outcome.

This argument proves the publication obligation for the stated one-push
fixture. Extending it to arbitrary reuse requires additional arguments about
index generations, speculative reads and arbitration. It does not justify
weakening other stores or fences.

## Executable check and source correspondence

[`weak_memory_campaign.py`](../scripts/weak_memory_campaign.py) generates an
adapter from the **current production header**. It preserves method bodies,
branches, early-empty probes, slot indexing, CAS operations and memory orders.
The only baseline substitutions are:

* `std::atomic<T>` becomes `ModelAtomic<T>`, derived from `rl::atomic<T>`;
  its default constructor explicitly records the C++20 value initialization
  as a relaxed store before simulated threads start. This gives Relacy's
  history an explicit initial slot value, without adding inter-thread ordering;
* memory orders and atomic fences use their `rl::` equivalents;
* `ModelAtomic::is_always_lock_free` is true for the simulation. Ordinary native
  builds remain responsible for the hardware lock-free static assertions.

The test is built without `DAGFLOW_FUZZ_HOOKS` or runtime diagnostics. No
controller mutex is inserted into the algorithm. The mutant additionally
changes only the push publication store from release to relaxed, using a unique
source anchor. Baseline and mutant use the same adapter and harness.

[`deque_relacy_tests.cpp`](weak_memory/deque_relacy_tests.cpp) uses two simulated
threads, capacity two, `rl::sched_full`, and an execution-depth limit of 200:

| Suite | Bound | Oracle |
| --- | --- | --- |
| `publication` | One push and one steal | Successful steal returns the item and sees non-atomic payload initialization without a race |
| `slot-publication` | One push and one steal, no payload read | Successful steal cannot return the initial slot value |
| `last-item` | One preloaded item, one owner pop, one steal | Unique custody, unchanged failed output, conservation including quiescent drain |
| `two-items` | Two preloaded items, one owner pop, two steal attempts | Unique custody and conservation across competing operations |

The mutation must produce a Relacy assertion or data-race report in **both**
publication suites. A surviving mutant fails the campaign. Build failures,
process timeouts, depth/livelock errors and other checker failures also fail;
none count as a detected mutation. The result JSON retains source/harness hashes,
Relacy revision, logs and counterexample histories. Adapted headers are saved
beside it, because temporary build paths in traces disappear after the run.

At the pinned revision, the four baselines completed **77, 77, 4,778 and 94,583**
executions respectively (**99,515** total). The relaxed mutant produced a
payload data race on execution 1 and a stale-slot assertion on execution 2.
The latter trace reads `bottom == 1` twice, reads slot value **0 [NOT CURRENT]**,
wins `top` CAS `0 → 1`, then fails `value == 1`. Both are actual checker reports,
not inferred from a timeout or from failing to finish the search.

## Run

Relacy is an optional external, header-only test dependency. Nothing is fetched
by CMake or by the runner. The validated revision is
`2c87447b56b90cbccb5129c93a4e82eda5e7868e`.

```sh
git clone https://github.com/dvyukov/relacy.git out/weak-memory/relacy
git -C out/weak-memory/relacy checkout 2c87447b56b90cbccb5129c93a4e82eda5e7868e
python3 scripts/weak_memory_campaign.py \
  --relacy out/weak-memory/relacy --cxx clang++
```

Optional CMake/CTest integration on UNIX with GCC/Clang accepts the directory containing
`relacy/relacy.hpp` (an absolute path is recommended):

```sh
cmake --preset core -B out/build/weak-memory \
  -DDAGFLOW_RELACY_INCLUDE_DIR="$PWD/out/weak-memory/relacy"
cmake --build out/build/weak-memory --target dagflow_weak_memory_campaign
ctest --test-dir out/build/weak-memory -L weak-memory \
  --output-on-failure --no-tests=error
```

The opt-in target and tests compile their own adapter binaries. Ordinary core
builds do not depend on Relacy. There is no sanitizer requirement: the checker
simulates memory observations rather than hoping native threads exhibit them.

Reports record the search configuration for each check. The larger idle and
shutdown models use explicit preemption bounds; the smaller scenarios retain
full search. Documented survivor controls are labeled `survivor-control` in
JSON and console output and never counted as detected mutations.

## Limits and next obligations

Full search here means completion of these finite scenarios in Relacy's model.
It does not mean all capacities, arbitrary operation sequences, C++ programs,
compiler transformations or hardware implementations have been proved correct.
The checked scenarios do not include ring reuse, multiple simultaneous thieves,
resize (unsupported by this deque), overflow or unbounded progress/fairness.
No production memory order is weakened by these tests.

The [parking and completion checks](weak-memory-parking-completion.md) now cover
bounded announce/sleep handshakes, final-credit publication and dependent
registration/propagation. The [idle accounting checks](weak-memory-idle-accounting.md)
cover cleanup visibility, multi-worker collection, parent/child handoffs,
multiple/repeated waiters and untimed wait notification. The
[shutdown composition](weak-memory-shutdown.md) checks admission drain followed
by physical drain, including a simultaneous idle waiter. The
[schedule explorer](schedule-exploration.md) remains useful for their bounded
control-flow transitions. A proof covering the whole runtime would additionally
need to compose the concrete queues, completion, parking, custody and storage
lifetime, beyond the abstract handoff and stop boundary checked here.
