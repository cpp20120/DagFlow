# Idle accounting weak-memory obligations

The [Relacy campaign](weak-memory.md) also checks the current
[`IdleAccounting`](../include/dagflow/detail/idle_accounting.hpp) implementation.
It tests physical packet retirement, descendant publication, and untimed idle
waiting. Completion readiness is a separate protocol: a packet's cleanup and
completion propagation must finish **before** its accounting retirement.

## Source mapping and preconditions

[`weak_memory_adapters.py`](../scripts/weak_memory_adapters.py) copies the class
declaration, including inline `publish_worker`, `publish` and `retire`, plus
`Total`, `publication_exhausted`, `publish_external`, `idle_locked`, `worker_idle`
and `wait` from the current sources. The atomic operations, counter collections,
branches, fences and CV notification remain intact. Atomic/mutex/CV operations
use their Relacy counterparts. Unique source anchors fail closed on incompatible
source changes. Generated adapters and source/harness hashes are retained with
the checker histories.

The [fixture](weak_memory/idle_accounting_fixture.hpp) provides fixed storage
for one or two worker lanes and at most one registered external producer.
Constructor/destructor allocation and registry/TLS management are replaced;
`producer_lane()` returns that already-registered lane without synchronization,
as a native cache hit does. The real linked-list traversal remains in the scan.
The registry and storage stay fixed until all simulated threads finish.

The accounting contract supplies these preconditions:

* Each lane has one writer, counters are monotonic and never wrap.
* Publication precedes queue transfer; queue acquisition conveys publication
  to the executor, including when another worker executes the packet.
* Packet destruction and completion propagation precede retirement. A child
  is published before its parent's retirement.
* The observer holds the registry mutex during a concurrent scan. Fixture
  `after()` checks run after all simulated threads, with no concurrent writers.

Initial accepted publications happen before simulated threads start. This
models an already-transferred root packet; the queue transfer itself is not
reimplemented here. Concurrent child publication uses the real inline method.
There are no controller locks around the worker's publication or retirement.

## Cleanup visibility and collection across workers

The worker writes its instrumented cleanup effect, then performs a release
retirement store. A scan that acquires that retirement value acquires the
preceding cleanup. The one-packet `visibility` fixture allows a false scan;
if the scan returns true, it must safely read the cleanup effect. It deliberately
does not call `worker_idle()`, so a notifier mutex cannot hide a broken release.

`aggregate` has two independent workers, each with one accepted packet, and a
third observer. For idle to be true, both retirement counters must be one:
neither can exceed one, and the publication total is two. The scan must acquire
both workers' effects. No shared retirement RMW aggregates them on its behalf.
Relaxing the retirement store produces a data race in both fixtures.
The synchronization rule is the release/acquire relation in
[C++ atomics.order](https://eel.is/c++draft/atomics.order).

## No false idle across a parent-to-child handoff

`handoff` starts with one live parent. Its worker publishes one child, then
retires the parent; the child remains live throughout the rest of the fixture.
There is consequently no idle point at which a true scan would be legitimate.
`external-handoff` places the initial publication in an external producer lane
and executes the parent on worker 1 of a two-worker pool. Publication and
retirement totals must work across these different lanes as well.

For these bounds, the executing worker's first retirement observation is either:

* Zero. To pass the stability check the second observation must also be zero.
  The known root publication already makes the publication total at least one,
  so the scan rejects idle.
* One. Its acquire imports the child's preceding publication; the subsequent
  publication scan must count root plus child. The retirement total is only
  one, so the scan again rejects idle.

The second load cannot move backward in the retirement modification order.
Across multiple monotonic lanes, equal observed sums imply equal observed
per-lane values. This concerns **observations**, not a claim that unseen stores
cannot occur between the collections.

Relaxing the **first** retirement acquire admits an old child-publication value
even if both retirement reads see one. The later acquire is too late to repair
the publication value already read. Removing the equality of the two retirement
collections instead admits old retirement, old publication, then new retirement.
Both mutations produce false-idle assertions in both handoff fixtures.

This is a proof argument for these bounded handoffs. Independent new submissions
may legitimately occur after an observed idle point. Stable shutdown additionally
requires the [admission drain](../docs/pool-lifecycle.md); this model does not
prove whole-pool linearizability. The [shutdown composition](weak-memory-shutdown.md)
checks the two barriers together with an abstract queue transfer.

## Announcement, retirement and CV enrollment

`wait` races the actual `wait()` against one final packet's cleanup, retirement
and `worker_idle()`. Name the relevant events:

| Waiter | Worker |
| --- | --- |
| A: increment `waiters_` under the mutex | R: release retirement store |
| Fw: SC announcement fence | Fp: SC fence in `worker_idle()` |
| L: first retirement acquire | W: relaxed waiter-count probe |
| Scan, then CV wait if non-idle | Mutex handoff and `notify_all()` if announced |

If W reads the initial zero, preceding A in coherence order, the SC fence
constraints require `Fp < Fw`. If L reads retirement preceding R, the same
constraints require `Fw < Fp`. Both misses would form a cycle, so they cannot
coexist. A probe reading zero after the waiter decremented is harmless: the
waiter already completed. This uses the fence/coherence constraints in
[C++ atomics.order](https://eel.is/c++draft/atomics.order).

When the worker observes the announcement, it takes and releases the same
mutex before notifying. The waiter holds that mutex from its predicate check
until the CV atomically unlocks and enrolls it. Therefore notification cannot
slip through the false-predicate/enrollment gap. After wake, the waiter checks
the counters again; a spurious wake does not establish idle.
[C++ condition-variable wait](https://eel.is/c++draft/thread.condition.condvar).

Removing either SC fence, the mutex handoff, or notification produces a Relacy
`DEADLOCK` counterexample. The native idle-accounting wait is already untimed:
this adapter does **not** remove a timeout as the parking fixture does. The
worker makes one no-work probe and terminates in this bounded scenario; another
task or another probe cannot repair a lost wake. The successful waiter must see
cleanup, and its waiter count must return to zero.

## Multiple and repeated idle waiters

`multiple-waiters` races two real calls to `wait()` with one worker's final
retirement and no-work notification. Both waiters must return with cleanup
visible, and the final waiter count must be zero. `repeated-waiters` makes each
waiter call `wait()` twice; the second call observes the same completed work,
without reopening submission or resetting accounting. This checks enrollment
and withdrawal when another caller can still be completing its earlier wait.

The mutex serializes waiter-count increments and decrements, while CV waits
release it. Counted waiters can thus coexist, and `notify_all` is required to
release every enrolled waiter after the last retirement. A caller arriving
after the notification instead checks the idle predicate under that mutex.
Replacing `notify_all` with `notify_one` strands one waiter; replacing the
count increment with `store(1)` loses an enrollment and fails the final balance
check. The [shutdown-with-waiter](weak-memory-shutdown.md) fixture also exercises
the same notification with one public `Pool::wait_idle()` and one shutdown.

These two additional idle fixtures use `rl::sched_bound`, with at most **two
preemptions of a runnable actor**. Blocking and thread-exit switches do not
consume this budget. All choices within that scheduler's bound are enumerated;
this is not unrestricted full search. The report records search type, bound
and execution depth per check. The original five fixtures keep `sched_full`.

At the pinned Relacy revision, condition-variable spurious wakeups are enabled
only for its random scheduler. These systematic runs do not enumerate them.
Repeated API waits do not stand in for spurious wakes. Atomic waits, used by
the shutdown composition, have their own bounded spurious-wakeup model.

## Bounds, results and reproduction

| Baseline suite | Actors | Search | Completed executions |
| --- | ---: | --- | ---: |
| `visibility` | 2 | Full | 129 |
| `aggregate` | 3 | Full | 238,694 |
| `handoff` | 2 | Full | 1,001 |
| `external-handoff` | 2 | Full | 7,034 |
| `wait` | 2 | Full | 7,630 |
| `multiple-waiters` | 3 | 2 preemptions | 4,130 |
| `repeated-waiters` | 3 | 2 preemptions | 6,876 |

Total: **265,494 baseline executions**, depth limit 200.
All **nine distinct mutations** are detected in twelve counterexample checks:
two data races, five invariant assertions and five lost-wake deadlocks.
Compiler: Clang 23.1.1; Relacy revision:
`2c87447b56b90cbccb5129c93a4e82eda5e7868e`.

```sh
python3 scripts/weak_memory_campaign.py --relacy out/weak-memory/relacy \
  --cxx clang++ --protocol idle_accounting \
  --report out/weak-memory/idle-accounting/report.json
ctest --test-dir out/build/weak-memory \
  -R '^dagflow_idle_accounting_weak_memory_tests$' \
  --output-on-failure --no-tests=error
```

The optional CMake target runs this protocol along with deque, parking,
completion and shutdown. CTest registers it separately under label `weak-memory`. A wall-clock
timeout, depth exhaustion, livelock, or compiler failure is a campaign failure,
never a detected mutant. Full search means these finite Relacy scenarios have
completed, not that all C++ executions or target architectures are proved.

This does not enumerate concurrent producer registration, cache eviction or
pool-address reuse, counter exhaustion/aggregate carry, wait exceptions,
more than two simultaneous idle waiters, new work between repeated waits,
arbitrary spawn chains, or storage reclamation. `Total` is copied unchanged, but these small
counts do not exercise its carry path. Native PBT/boundary/lifecycle tests remain
responsible for those separate obligations. Shutdown has its own
[bounded composition and limitations](weak-memory-shutdown.md).
