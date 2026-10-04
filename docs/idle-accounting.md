# Pool idle accounting

`Pool::wait_idle()` observes completion of task packet epilogues, including
callable destruction and completion propagation. It is external-only and does
not close admission: a concurrent submit may extend the wait, or start work
after the observed idle point. [Pool shutdown](pool-lifecycle.md) first closes
external admission and waits for admitted publishers, then uses this accounting
barrier before stopping workers. Task/capture lifetimes remain caller obligations.

## Writers and ownership

`IdleAccounting` owns a contiguous cache-line-aligned worker lane array and a
list of external producer lanes, all allocated through `runtime_memory`.
A worker exclusively writes its own `published` and `retired` counters; an
external thread exclusively writes its producer lane's `published` counter.
Counts are monotonic. Updates are a relaxed owner load plus a release store,
not a shared fetch-add/sub. Worker updates are inline and select the lane by
worker ID. The [TLS worker-lane cache experiment](experiments/worker-lane-cache.md)
was removed after its measurements failed to establish an overall benefit.
The external producer registration cache described below remains active.

Publication is counted after packet preparation and producer registration, but
before queue publication. Preparation failure destroys the unpublished owned
packet and retires its credit. The executing worker counts retirement after
callable/packet destruction and `CompletionCredit::finish()`. Thus a child
published during execution, callable destruction or completion propagation is
counted before its parent's physical retirement.

Stealing does not transfer a balance or change metadata: publication belongs to
the sender, retirement to the executor. Task packets carry no accounting pointer
or lane ID. Handles, scopes and graph credits keep their own existing completion
protocols; pool accounting observes the later physical epilogue.

## Producer registration

A thread registers once per pool under the registry mutex. A bounded four-slot
TLS cache holds pool identity/lane pairs. Cache hits neither allocate nor lock.
Eviction falls back to registry lookup by unique producer ID and reuses the same
lane. IDs are never reused; a stale TLS pointer is not dereferenced when a pool
is reconstructed at the same address. Producer thread exit does not reclaim a
lane: accumulated counts and storage remain owned by the pool until destruction.
The list is destroyed iteratively to avoid recursive destruction under churn.

On this 64-bit build, each worker lane occupies 64 bytes and each registered
producer node 128 bytes. Memory and scan cost grow with the number of distinct
external threads that have submitted to this pool. The four-entry TLS cache is
bounded, but the pool's producer registry is not. Registration failure leaves
no counted publication. This is deliberately a pool-lifetime design, not an
allocator or producer-record reclamation protocol.

## Idle observation

A waiter holds the registry mutex, preventing an unseen new producer lane from
appearing during a scan. Existing lane owners continue publishing and retiring.
It acquires counters in this order:

1. Collect all worker retirement counts into `before`.
2. Collect all worker and registered producer publication counts.
3. Collect worker retirement counts again into `after`.
4. Accept idle only if `before == after == published`; otherwise wait and retry.

Retirement counters cannot decrease or wrap. Equal retirement sums in both
collections therefore imply equal observed values in every retirement lane;
a completion appearing during collection invalidates that attempt. The
publication collection sits between these two retirement collections. Acquire
loads of retirement carry the corresponding task's publication and preceding
child publications into the later publication reads. Reading publications first
could instead combine an old publication total with newer completions.

Only an idle observation is required, not a snapshot that reports an exact
nonzero outstanding count. Independent concurrent submissions may fall after
the observed idle point, as with the previous API. This is not a completion
barrier for submissions that have not yet been accepted.

Totals use two 64-bit words with carry, so summing many lanes does not alias zero
through a 64-bit sum wrap. Per-lane publication exhaustion throws before queue
publication; retirement or unique-ID exhaustion terminates rather than wrapping.
This preserves the monotonicity and cache-identity assumptions.

## Waiting without a per-task notification counter

`wait()` announces itself under the registry/CV mutex, executes an SC fence,
then performs its idle check and enters CV wait if necessary. When a worker
finds no task on its normal acquisition attempt, `worker_idle()` executes the
matching SC fence and probes the waiter count. With no waiter it returns. With
waiters it takes/releases the same mutex and notifies all.

The fence pair prevents both the waiter from missing retirement and the worker
from missing the wait announcement. The mutex handshake covers predicate check
to CV sleep. There is no per-task increment of a shared notification epoch, no
per-task waiter-count probe, and no timeout in accounting wait. Worker parking
retains its separate timed protocol. Repeated failed acquisitions may generate
spurious accounting notifications; the waiter always checks counts again.

Notification moves to the worker's no-work path, so `wait_idle()` may resume
slightly later than the final retirement store (after a scheduler acquisition
attempt). Concurrent waiters are supported. User code cannot invoke it from
one of the pool's workers; cooperative handle/scope waits remain unchanged.

## Verification

Tests cover publication-registration failure, cache hits and eviction without
extra allocation, producer exit, pool address reuse, concurrent registration,
multiple waiters, destructor-spawned children, nested handoffs with no persistent
root task, retirement/wait races and long worker backoff. Existing queue overflow,
completion exception, graph and scope tests exercise the shared enqueue/retire
paths. Sanitizer coverage does not replace weak-memory hardware validation.

See [the measured comparison](benchmarks/idle-accounting.md) for results and limits.
Rebuild consumers for the changed `Pool` layout. Manual builds must also compile
`src/idle_accounting.cpp`; CMake includes it automatically.
