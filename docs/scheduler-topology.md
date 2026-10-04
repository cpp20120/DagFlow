# Scheduler domains and memory

Scheduler construction allocates three contiguous arrays through `runtime_memory`:
`Local[]`, `Shard[]`, and a CSR list of worker IDs. Every worker has an immutable
`home_shard`. Parking uses the same membership map, so a shard is an injection,
work-locality and parking domain. These are logical groups, not detected CPU,
NUMA or LLC domains.

```cpp
dagflow::Config config;
config.threads = 8;
config.shards = 2; // Default mapping: workers 0..3 → 0, 4..7 → 1.
config.pin_threads = false;
dagflow::Pool pool(config);
```

For explicit groups, set `Config::worker_shards` to one shard ID per worker.
For example, `{1, 0, 1, 0}` assigns four workers to two interleaved groups.
The constructor rejects incorrect lengths or IDs outside the shard domain before
starting threads. An empty mapping selects `floor(worker_id * shards / workers)`.
`shards=0` means the normalized worker count; zero requested workers means one.
More shards than workers, and empty domains in explicit maps, are supported.

`SubmitOptions::affinity` names a worker. Its ingress shard is looked up through
that worker's mapping; oversized hints wrap by worker count before lookup.
External submissions with no hint rotate over all shards, including empty ones.
A worker's ordinary spawn prefers its local deque, then home ingress. Explicit
remote affinity publishes into the target domain; `SubmissionMode::Enqueue`
always uses ingress. The publication and wakeup use the same selected shard.
Affinity remains a hint: stealing, helping and overflow acquisition can execute work
elsewhere. Consumers must rebuild after the `Config`/`Pool` layout change.

## Acquisition

A worker normally tries its local queues, home ingress, and home-shard peers,
then remote domains. Each domain prefers High to Normal within its tier; this
is not a global priority ordering. Victims and the initial remote domain use a
small owner-only SplitMix64 state (8 bytes), replacing `mt19937` and its per-worker
initialization. Randomness is scheduling dispersion, not a security mechanism.

The independent ingress probe remains every 32 acquisitions, rotates its initial
shard, and prefers High work at that probe. Thus a perpetually nonempty local
queue cannot suppress external work of equal priority indefinitely.

An ingress drain returns one task directly and places the rest of its bounded
batch into the worker's Chase–Lev deque before invoking user code. Stealing also
publishes siblings before executing its first acquired task. This preserves
nested helping when one task waits on another member of the same batch.

Every successful ingress acquisition or steal asks Pool to relay one wake before
user code. This includes a one-task result: draining the selected source does
not prove that other queues are empty. Otherwise recruitment can stop with
available work and sleeping workers. A local owner pop consumes already-covered
work and does not request another wake. If acquisition follows an idle
announcement, Pool cancels its own bit before relaying, so it cannot select
itself. Submission wakes and ParkingLot's handshake remain unchanged. See the
[drain/wake experiments](experiments/drain-wake.md) for the rejected bulk-drain
variants and the eight-worker recruitment regression.

Chase–Lev and ingress remain fixed and bounded. Worker saturation spills into
a per-shard, per-priority intrusive FIFO, visible to every worker and helper.
Empty overflow queues need only an atomic probe; links use a mutex. The packet
owns its link, so publication allocates no queue nodes. Overflow drain publishes a bounded batch into the owner deque
before returning its first task, under one overflow lock. Shared acquisition
alternates ingress and overflow when both are available and relays the ordinary
wake. Periodic probes keep source preference for one full shard rotation, then
flip it, independently of ordinary drains. This prevents permanently pairing
ingress in one busy shard with overflow in another and starving the first
shard's overflow. External producers retain backpressure.
[Idle accounting](idle-accounting.md) uses single-writer publication/retirement
lanes. [Shutdown](pool-lifecycle.md) closes external admission and drains before
stopping workers.

## ParkingLot

`ParkingLot` owns contiguous `Waiter[]`, `Domain[]`, and exactly
`ceil(workers/64)` atomic idle words. Bits follow the scheduler's CSR worker
order, so a domain is a contiguous bit range, even for an interleaved explicit
worker map. Domain word ranges and endpoint masks are computed at construction.
A domain can share its first/last word with other domains.

For up to 64 workers, `wake_one(shard)` performs a publication fence and one
relaxed idle probe. Zero bits return immediately, without a shared RMW or epoch
update. Otherwise it prefers the cached domain mask and falls back to another
idle bit in that same word. Larger pools first probe masked home-domain words,
then scan the compact word array. No-idle work is O(ceil(workers/64)), independent
of empty-domain count; no universal constant-time bound is promised.

A successful CAS claims one registration and signals its worker. Competing
publishers observe the claimed bit cleared: only one notification is needed
until the owner registers again. This coalesces notifications without a separate
`need_wake` flag. Every submission still publishes immediately; there is no hidden
TLS task buffer or changed flush requirement. Remote recruitment remains valid
when the selected domain is occupied or has no resident worker.

The lost-wakeup protocol is:

1. An owner snapshots its wake epoch, announces its idle bit, then executes an
   SC fence before the final scheduler acquisition.
2. A publisher enqueues work, executes an SC fence, then probes idle words.
   The two fences prevent the owner from missing publication while the publisher
   also misses registration. If both reads observed the preceding values, they
   would require contradictory ordering of the two SC fences. A bit already
   cleared by another notifier has transferred wake responsibility to that
   notifier instead.
3. If the owner finds work, it cancels registration **before** user code.
   Otherwise it waits under its mutex with predicate `stop || epoch changed`.
4. The publisher claims an idle bit before incrementing that waiter's epoch.
   An announced worker first spins for at most 128 pause iterations on its own
   epoch, without rescanning queues. Before entering CV wait it stores
   `sleeping=true` under the waiter mutex, then checks the epoch predicate.
   The notifier takes/releases the mutex and notifies only if it observes
   `sleeping=true`. Epoch increment/probe and sleeping announcement/predicate
   use SC ordering: either the worker observes the new epoch, or the notifier
   observes the sleeping announcement. Thus a worker still scanning/spinning
   needs only the epoch update, while a real sleeper retains the mutex handshake.
5. A claimed registration is never cleared again by its notifier: the owner
   may already have registered another wait. The new registration either sees
   the later epoch update or remains eligible for a subsequent publisher.
6. No-idle submission no longer performs a representative-worker RMW/epoch
   fallback. Its replacement is the paired-fence protocol, **not** a standalone
   relaxed `if (idle == 0) return`. Shutdown still signals every worker and
   preserves the stop predicate and mutex handshake.

Spurious notifications and timed wakes are valid: the worker always retries
acquisition. The CV/backoff mechanism remains after the bounded spin. The spin
bound is an initial implementation choice, not a measured optimum; it trades
some active CPU time for fewer short sleeps. Successful acquisition still relays
recruitment before user code. Empty steal probes reject before the arbitration
fence; possible successes retain the fence, bottom recheck and last-item CAS.

## Allocator contract

Task packets and completion states still allocate their actual size/alignment
through `runtime_memory`. Size-class selection and remote free belong to the
selected library; there is no additional DagFlow slab/free-list or class table.

For mimalloc, naturally aligned object-size requests use `mi_malloc_small` up to
`MI_SMALL_SIZE_MAX`, and `mi_malloc` above that. The eligibility check requires
alignment at most `alignof(max_align_t)`, size at least alignment, and a size
multiple of alignment. Other buffers and over-aligned objects retain
`mi_malloc_aligned`. All are released with `mi_free`, including from another
thread after the allocating thread exits. Task packets vary with callable size;
eligible packets and completion states take the native small-object path.

`tbbmalloc` retains `scalable_aligned_malloc/free`, using that backend's own size
classes. `system` remains exact ordinary/aligned `operator new/delete`, useful
for sanitizers and fault injection. It does not emulate size classes. No class
policy leaks into container or callable templates.

## Validation

- Allocation recording checks exactly three Scheduler array allocations and
  three ParkingLot array allocations, independent of worker count. Failure at
  each of these six points releases all partially constructed storage.
- Topology tests check custom membership, affinity mapping, home-before-remote
  stealing, empty domains, and ingress batch visibility.
- Parking tests cover domains spanning multiple mask words, signals preceding
  waits, repeated publication/registration races, and long-backoff pool bursts
  with nested cross-domain scope spawning.
- Allocator tests hold many simultaneous blocks at small-size boundaries and
  multiple alignments, then free them after the producer thread exits. System
  fault injection also covers partial Pool construction and thread startup.
- Benchmark suite `--shards N` controls domain count independently of workers;
  zero preserves the default of one shard per worker. For example:

```sh
python3 scripts/benchmark_suite.py --workers 4,8 --shards 2 \
  --scenarios local_saturated,steal_heavy,idle_burst,scope_recursive
```

See [the initial comparison](benchmarks/memory-topology.md) and the
[wakeup profile and follow-up](benchmarks/wakeup-profile.md) for measurements
and their limits.
