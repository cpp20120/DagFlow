# Shutdown and idle accounting composition

This [Relacy campaign](weak-memory.md) composes the current pool admission and
shutdown methods with the [idle-accounting implementation](weak-memory-idle-accounting.md).
It checks two barriers: dispatch by accepted external publishers must finish,
then physical packet epilogues must drain, before shutdown publishes stop.

## Source correspondence

The adapter copies `Pool::PublicationGuard`, its constructor/destructor,
`enter_publication`, `leave_publication`, `close`, `closed`, `wait_idle` and the
**complete `shutdown()` body** from the current header and source. The admission
closed bit, admission counter, shutdown mutex and stop flag declarations are
also copied. It embeds the source-derived `IdleAccounting` adapter unchanged.
Synchronization uses Relacy atomics, mutexes and CVs; no controller mutex is
inserted into the publication or worker epilogue paths.

The [fixture](weak_memory/shutdown_fixture.hpp) replaces `parking_->wake_all()`
with an effect-check callback at that exact point in shutdown. It supplies an
empty thread array: OS join and worker parking are outside this model. This
deliberately checks visibility **before join**, so thread-join synchronization
cannot repair a broken barrier. Storage remains pinned until all actors finish.
All modeled `PublicationGuard`, `wait_idle` and shutdown callers are external;
the TLS worker test is fixed to false. The misuse exception paths are not tested.

The harness represents task effects and physical cleanup with `rl::var` writes.
It calls the real accounting publication/retirement bodies, but does not copy
`execute_task`, exception dispatch, completion propagation or scheduler queues.
Those are composition preconditions, not newly verified implementations here.

## Checked scenarios

| Suite | Concurrent actors and obligation |
| --- | --- |
| `admission` | One already-admitted publisher finishes rollback without a packet; shutdown must acquire its cleanup even though accounting is empty. |
| `close-race` | An external `PublicationGuard` races shutdown from open admission. Exactly one accepted/rejected result occurs; an accepted guard keeps stop false until it releases its reservation. |
| `descendant-drain` | One already-dispatched root publishes a child while shutdown drains. Root and child cleanup must both be visible at stop/wake; neither task may start under stop. |
| `publication-drain` | Three actors: an admitted but not-yet-counted publisher, its worker, and shutdown. Both barriers operate in the same execution. |
| `shutdown-with-waiter` | A worker retires one packet while ordinary `Pool::wait_idle()` and shutdown both wait on the accounting CV. Both must return with cleanup visible and the waiter count balanced. |

In `publication-drain`, the publisher initializes payload, counts an external
publication, release-stores a one-shot mailbox, notifies the worker, then calls
`leave_publication`. The worker acquires that mailbox through atomic wait, checks
stop, reads payload, writes cleanup, retires and makes one no-work notification.
The mailbox abstracts the queue's publication/acquisition relation; it does not
model capacity, arbitration, stealing, retries, or the parking protocol. This
case has one packet; the descendant case separately composes parent and child
retirement. There is no claim of a full producer/queue/descendant/shutdown product.

## Ordering argument

The successful reservation CAS and close's RMW participate in one admission
modification order. A reservation that wins first remains counted after close;
a CAS racing after close must fail and observe closed before retrying. Shutdown
therefore waits for accepted publishers rather than mistaking temporarily empty
accounting for a completed drain.

For the single-publisher fixtures, a release decrement publishes dispatch or
rollback effects. Shutdown either acquires that release through close's RMW
when the publisher finished first, or through a later admission observation.
It reaches physical drain only with closed admission and no publisher left.
The accounting scan therefore starts with knowledge of accepted publication;
it cannot accept the old empty state while that accepted packet is still pending.
This uses release/acquire and RMW ordering in
[C++ atomics.order](https://eel.is/c++draft/atomics.order).

After admission drain, descendants remain possible, but they are counted before
their live parent retires. The [retirement/publication collection](weak-memory-idle-accounting.md)
must cover the parent-to-child handoff. Physical drain acquires cleanup before
returning; only then does the copied shutdown body store stop and call wake.
The fixture checks effects at wake and checks stop before worker execution.

An ordinary idle waiter does not close admission. It can share the CV and count
with shutdown's idle wait because enrollment/withdrawal are under the same
mutex. A final `notify_all` serves both enrolled callers; `notify_one` can leave
one stranded with no future retirement notification.
[C++ condition-variable notification](https://eel.is/c++draft/thread.condition.condvar).

These arguments apply to the listed finite shapes and their queue/lifetime
preconditions, not to all accepted publisher counts, nested graphs or shutdown
callers.

## Mutations and the surviving control

| Variant | Required observation |
| --- | --- |
| `shutdown-drop-admission-drain` | Data race in rollback fixture; early stop assertion in publication/drain composition |
| `shutdown-relax-publisher-release` | Publisher cleanup data race |
| `shutdown-drop-physical-drain` | Cleanup data race in both drain fixtures |
| `shutdown-stop-before-drain` | Worker observes stop before accepted work in both drain fixtures |
| `shutdown-idle-before-admission` | Early stop after an idle check made before accepted publication |
| `shutdown-ignore-closed` | A guard is accepted after shutdown has stopped the pool |
| `shutdown-notify-one` | One of the idle/shutdown waiters remains blocked: model deadlock |
| `shutdown-relax-admission-and-wait-fence` | Publisher cleanup data race after both acquisition mechanisms are weakened |
| `shutdown-relax-admission-acquire` | **Survives** the `admission` fixture; recorded as `survivor-control` |

The surviving control relaxes the three admission observations inside shutdown,
including atomic wait, while retaining publisher release, close's acq_rel RMW,
and accounting's announcement fence. Its survival does not count as mutation
detection. There is a source-level reason that the later fence can preserve
visibility: a relaxed load reading a release operation, followed by an acquire
fence, can synchronize with that release. Accounting's SC fence has acquire
semantics. Removing that fence **as well** yields the compound counterexample.
[C++ atomic-to-fence synchronization](https://eel.is/c++draft/atomics.fences).

This is not permission to weaken production orders: the control covers one
publisher and one rollback shape. Relacy's atomic-wait implementation also uses
internal SC fences; its implementation is not a proof that every standard-library
atomic wait has the same extra ordering. The runtime's orders remain unchanged.

Eight variants have counterexamples in eleven checks; one explicitly documented
control survives. JSON and console output distinguish `baseline`,
`counterexample`, and `survivor-control`. Unexpected survival of a negative
control, unexpected failure of a baseline/survivor control, process timeout,
depth exhaustion or infrastructure error fails the campaign.

## Search bounds and observed results

| Baseline suite | Actors | Search | Completed executions |
| --- | ---: | --- | ---: |
| `admission` | 2 | Full | 14,176 |
| `close-race` | 2 | Full | 111,074 |
| `descendant-drain` | 2 | 2 preemptions | 1,172 |
| `publication-drain` | 3 | 1 preemption | 1,760,254 |
| `shutdown-with-waiter` | 3 | 2 preemptions | 13,500 |

Total: **1,900,176 baseline executions**, depth limit 200. Larger scenarios use
Relacy's systematic context-bound scheduler. A preemption is a switch away from
a runnable actor; switches forced by blocking or thread completion do not
consume the bound. Results are exhaustive within that scheduler's stated bound,
not unrestricted full search. Reports record the configuration per check.

The pinned Relacy revision is `2c87447b56b90cbccb5129c93a4e82eda5e7868e`, compiled
with Clang 23.1.1. Its systematic CV checks do not generate spurious CV wakeups;
atomic wait separately permits up to five spurious wakes per call. The latter
choices remain part of these executions. Fairness, arbitrary delays and
unbounded progress are outside the claim.

```sh
python3 scripts/weak_memory_campaign.py --relacy out/weak-memory/relacy \
  --cxx clang++ --protocol idle_accounting --protocol shutdown \
  --report out/weak-memory/idle-shutdown/report.json
ctest --test-dir out/build/weak-memory -L weak-memory \
  --output-on-failure --no-tests=error
```

CTest adds `dagflow_shutdown_weak_memory_tests` when Relacy is configured. Normal
core builds remain dependency-free. Concurrent shutdown calls, OS joins,
publisher registration/allocation, counter overflow, worker-origin shutdown
misuse, new submissions between repeated waits, and physical storage reclamation
remain covered by their native tests rather than this bounded composition.
