# Parking and completion weak-memory obligations

These checks extend the [deque campaign](weak-memory.md) with source-derived
parking and completion models. The runner copies current method bodies from
`src/parking_lot.cpp` and `src/thread_pool.cpp`, switches synchronization to
Relacy, and retains each generated adapter and counterexample in its report.
Native runtime files are not modified by the campaign.

## Parking: publication and idle announcement

The fixture has one worker, one idle bit, and one persistent publication.
An atomic release/acquire publication represents the queue's availability; a
non-atomic payload checks its visibility. Queue custody is outside this model.

Name these events in [`ParkingLot`](../src/parking_lot.cpp):

| Waiter | Publisher |
| --- | --- |
| A: `prepare()` sets its idle bit with SC `fetch_or` | Q: queue publication |
| Fw: SC fence in `prepare()` | Fp: SC fence in `wake_one()` |
| Rq: final availability scan | Ri: relaxed idle-word probe |

Suppose both scans miss. Ri reads an idle value preceding A. Since Fp precedes
Ri, the SC constraints require `Fp < A < Fw`. Rq reads the availability value
preceding Q; with Fw before Rq and Q before Fp, the fence constraints require
`Fw < Fp`. The cycle is impossible. Therefore either the final scan finds the
publication or the publisher observes the idle bit. In this single-bit fixture,
with no cancellation on a missed scan, claiming that bit increments the epoch.
The `announce` oracle checks this alternative after both threads finish.
The ordering rules used are the SC coherence/fence constraints in
[C++ atomics.order](https://eel.is/c++draft/atomics.order).

Removing the publisher fence admits both misses; Relacy reports an assertion
failure. A release/acquire queue publication by itself does not suffice when
the acquire scan reads the old value.

## Parking: epoch, sleeping and CV enrollment

At the sleep boundary, the waiter holds its mutex, stores `sleeping = true`
with SC ordering, and checks epoch with SC ordering. The notifier increments
epoch with an SC RMW, then probes sleeping with an SC load. If the waiter read
the old epoch and the notifier read sleeping as false, their program orders
and the two stale observations would form another cycle in the SC order:

```text
sleeping=true < old-epoch read < epoch increment
             < sleeping=false read < sleeping=true
```

Thus either the predicate already sees the signal, or a notifier of the claimed
registration must perform the mutex/notify path. Taking and releasing the
waiter's mutex before notify closes the interval between the false predicate
and atomic unlock/enrollment in the CV wait. This uses the CV wait contract,
not an assumption that notify remembers a notification sent before enrollment.
[C++ condition-variable wait](https://eel.is/c++draft/thread.condition.condvar).

The adapter uses the **actual wait body and actual signal body**, but makes
two explicit reductions:

* `epoch_spin_count = 0`: the model focuses on reaching the CV boundary without
  observing a signal in the optional spin phase. It does not enumerate 128
  equivalent failed spin probes or claim to verify that optimization.
* `wait_for` delegates to an untimed Relacy predicate wait. The native timeout
  can recover from a missed notification; it must not hide that violation in
  this check. At the pinned revision Relacy enables CV spurious wakeups only
  for random search, so these systematic runs do not enumerate them.

The `sleep` fixture announces before the two simulated threads start, then
races actual wait against `wake_worker`. The `wake` fixture includes actual
prepare, final scan, wait and `wake_one`. On return, the waiter must see the
publication and payload and must have cleared its sleeping state.

Lost-wake mutants produce **Relacy `DEADLOCK` counterexamples in the untimed
model**. This is not a claim that the native timed waiter blocks forever.
A process timeout, execution-depth limit or livelock report never counts as a
detected mutation. The JSON distinguishes the checker status and exit code.

## Completion: retirement, cleanup and readiness

The precondition is a valid positive credit balance: each actor owns exactly
one live credit, effects precede its retirement, and no credit is created after
terminal zero. The fixtures initialize that balance before starting threads;
they do not model arbitrary concurrent `fork()` or scheduler publication.

In [`CompletionState::retire`](../src/thread_pool.cpp), credit decrements are
acquire/release RMWs on the same counter. In the two-credit fixture the last
decrement reads the preceding decrement, acquiring that actor's effects. More
generally, intervening RMWs form release sequences; this is the ordering needed
to collect earlier retirements before the last credit destroys the payload.
Cleanup is sequenced before `ready.store(true, release)`. An observer that
reads true with acquire therefore sees the effects and completed cleanup.
[C++ release sequences and synchronization](https://eel.is/c++draft/atomics.order).

The executable obligations are separate so an unrelated mutex cannot mask a
missing edge:

* `aggregate`: two credit owners write separate instrumented effects; the final
  payload destructor reads both and runs once. There is no error mutex path.
* `ready`: one owner retires; an observer performs one acquire readiness probe
  and checks effects/cleanup if true. A false probe has no progress obligation.
* `wait`: the observer uses the copied `CompletionState::wait()` and must return
  with effects and cleanup visible. This exercises Relacy's atomic-wait model;
  the separate readiness probe provides the weak-publication negative control.

Making the initial retirement decrement release-only loses collection of the
other owner's effects. Making readiness publication relaxed loses visibility
to the observing thread. Each has a data-race counterexample on `rl::var` data.

## Completion: early and late dependent registration

`add_dependent` holds the source mutex while claiming the atomic gate and
appending an edge. The final credit publishes ready, then exchanges the gate
to closed:

* If the exchange observes registered, the finalizer locks the same mutex
  before taking the edges. This covers registration that claimed the gate but
  has not yet appended its edge.
* If the registrar observes closed, its failed CAS uses acquire ordering.
  Reading the finalizer's release exchange acquires source cleanup. The late
  edge can retire immediately without a ready wait. The source need not have
  acquired the registration mutex on this path, so the gate ordering matters.

`registration` races these operations and makes dependent cleanup read the
source-cleanup effect. It also checks exactly-once delivery, terminal credits
and reference balance. Relaxing either the closed-gate exchange or the failed
CAS observation yields a data race. `registration-error` additionally checks
the immutable error identity arriving on early and late edges. Its mutex path
is not used as the sole visibility oracle, because it could hide a gate bug.

## Completion: effects arriving through dependent edges

The dependent credit decrement in the terminal worklist is another acquire/
release RMW. Two already-terminal sources can deliver their edges in either
order; the final dependent retirement must acquire both sources' effects before
its payload cleanup. The `cascade` oracle checks those effects and exactly-once
cleanup. Weakening that decrement to release-only produces a data race.

To keep full search finite in practice, this fixture starts at the two edge
arrivals. The runner extracts the actual edge-delivery block, and splits the
actual terminal-worklist body into a helper callable when the count reaches
zero. The ordinary modeled `retire()` uses the same helper. No atomic operation
or memory order is added by the split. Both copies of the dependent decrement
are mutated together. The two complete source epilogues are not jointly
enumerated here; retirement and registration have their own fixtures.

## Source mapping and limits

[`weak_memory_adapters.py`](../scripts/weak_memory_adapters.py) extracts balanced
definitions with unique source anchors. Source changes that invalidate an
anchor fail the campaign. Reports include hashes of native sources, harnesses,
adapter scripts, the compiler version and the Relacy revision.

Parking copies `Waiter`, `Domain`, prepare/cancel/wait/signal, wake_one,
wake_worker and word/domain wake helpers. Its storage is fixed at one worker
and one word. Membership and domain construction, multiple waiters, epoch wrap,
stop/shutdown, allocation failures and physical reclamation are outside this
fixture. Runtime diagnostics and fuzz hooks are disabled.

Completion copies the credit/state declarations and retain/release, set/get
error, add_dependent, retire, wait, fail and finish bodies, plus `Handle::ready()`
as a model getter so changes to its acquire order also affect the check. An observing reference
pins each state in stack fixture storage until the simulated threads finish.
The model deleter checks terminal invariants but does not free that storage.
Construction of an already-owned credit is exposed to the fixture. Error identity
is an instrumented integer instead of exception_ptr internals. User effects,
cleanup and error accesses use `rl::var`; raw list/worklist fields and std::vector
internals are not independently instrumented for arbitrary data races. Worker
waiter lists are empty. This is not allocator, refcount reclamation, arbitrary
fork, error-object lifetime, or worker-waiter registration verification.

The checked shapes have two simulated threads, bounded work and a 200-step
execution limit. Full search means the finite scenarios completed in Relacy's
model; it is not a formal proof of every C++ execution of the whole runtime.

## Observed results and commands

| Baseline suite | Completed executions |
| --- | ---: |
| Parking announce | 2,732 |
| Parking sleep | 9,023 |
| Parking wake | 492,859 |
| Completion aggregate | 50 |
| Completion ready | 80 |
| Completion wait | 23,171 |
| Completion registration | 5,374 |
| Completion registration-error | 26,757 |
| Completion cascade | 50 |

Total: **560,096 baseline executions**. Four parking mutations and five completion
mutations are detected. Three parking cases report a model deadlock; the
publisher-fence case reports an assertion; all five completion cases report a
data race. These statuses are expected negative controls, not failing baselines.

```sh
python3 scripts/weak_memory_campaign.py --relacy out/weak-memory/relacy \
  --cxx clang++ --protocol parking --protocol completion
python3 scripts/weak_memory_campaign.py --relacy out/weak-memory/relacy \
  --cxx clang++ --protocol completion --mutant completion-ready-relaxed
```

Without `--protocol`, the campaign also checks deque,
[idle accounting](weak-memory-idle-accounting.md) and
[shutdown composition](weak-memory-shutdown.md). The existing opt-in
`dagflow_weak_memory_campaign` target runs all protocols; CTest registers
separate `dagflow_{deque,parking,completion,idle_accounting,shutdown}_weak_memory_tests`, each labeled
`weak-memory`, with separate report directories. See the
[setup commands](weak-memory.md#run). Native core tests remain dependency-free.
