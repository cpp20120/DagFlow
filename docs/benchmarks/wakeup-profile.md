# Wakeup profile and compact idle probe

The later [single-writer accounting change](idle-accounting.md) addresses the
shared counter identified here; this report preserves the preceding snapshot.

Measured 2026-09-30, following the [memory/topology comparison](memory-topology.md).
The expensive no-idle wakeup fallback was confirmed by sampling and removed.
This reduces the external-submit regression, but does not eliminate the whole
regression against the implementation before the topology rewrite.

## Evidence before the change

`perf record -e cycles:u -F 999`, external detached submission, one producer,
four workers/shards, 4,096 tasks per run, 2,000 measured runs and 20 warmups:

| Function | Before (% sampled cycles) | After (% sampled cycles) |
|---|---:|---:|
| `ParkingLot::wake_one` | 13.50 | 4.65 |
| `ParkingLot::wake_domain` | 6.23 | Below 1% reporting threshold |
| `Scheduler::drain` | 11.23 | 12.83 |
| `Scheduler::try_acquire` | 6.37 | 6.95 |
| `Scheduler::steal` | 7.12 | 6.18 |
| `Pool::enqueue` | 6.05 | 8.52 |
| `Pool::execute_task` | 6.25 | 7.81 |
| `mi_free` | 10.36 | 19.05 |

These are shares from separate samples, not absolute per-call costs. A larger
share after removing wakeup overhead does not establish that a function slowed
down. Mimalloc exports `mi_free` and `operator delete[](void*)` at the same address;
perf labels that region as the latter. This is not evidence of an array-delete bug.

Before, the hottest annotated region in `wake_one` was the no-idle fallback:
a `lock cmpxchg` loop clearing a representative waiter's bit, followed by a
`lock inc` on its epoch. This still ran when every worker was active. The preceding
path walked domains, performed modulus operations, and fetched membership from
Scheduler. Worker batch processing also invoked wakeup while local work remained;
this overhead was not restricted to calls from the external producer.

## Implemented change

- Bits are packed by CSR worker order into exactly `ceil(workers/64)` words.
  Domain word ranges and first/last masks are computed once at construction.
- With at most 64 workers, submission performs one SC fence and one relaxed
  bitmap load. A zero bitmap returns immediately: no domain walk, shared RMW,
  membership lookup or epoch write.
- With idle workers, the cached domain mask selects a local bit if possible.
  Otherwise a remote bit from the same loaded word is used. A successful CAS
  claims the registration; competing publishers need not notify it again.
- Larger pools check the masked home range, then compact words. Empty shard
  count no longer multiplies the global fallback scan.
- Waiter registration has the matching SC fence before its final acquisition.
  Claimed notifications retain epoch publication and the mutex/CV handshake.

The [parking contract](../scheduler-topology.md#parkinglot) explains why both
publisher and waiter cannot miss each other's publication/registration. A lone
relaxed zero-probe would not supply this guarantee. The compiler here lowers the
SC fence to `lock orl $0, stack-slot`: synchronization remains, but it touches the
publisher's stack instead of modifying a remote waiter's hot state on every task.

Notification coalescing uses the existing idle registration as the claim token;
there is no separate `need_wake` flag with another rearming protocol. Task
publication remains immediate. No hidden TLS task buffer was introduced: an
unflushed final task would change submit/wait progress and ownership semantics.
An explicit batch API would be a separate, measurable change.

## Controlled experiments and throughput

Three variants were checked in alternating runs: the pre-change implementation,
paired fences with the old word layout, and paired fences with compact masks.
The final variant retained the mask optimization. Raw intermediate results are
`fence-probe.jsonl` and `compact-probe.jsonl`; short-run gains varied enough that
only the broader final comparison is reported below.

Linux x86_64, Ryzen 7 6800H, Clang 22.1.8, mimalloc 3.5, static Release, no
LTO/PGO/native tuning. No pinning, frequency lock or CPU isolation. The final
matrix uses three paired trials, each with three warmups and 21 measured runs,
4,096 task units and exactly 0 or 400 body iterations. Points are shuffled and
binary order alternates; all paired checksums match. Ratios use the median of
three run medians. They are throughput-mode whole-run times, not per-submit
latency or p99/p999 claims.

Four workers and four shards, zero body iterations:

| Scenario | Before wake fix (µs) | After (µs) | After/before | After/original topology |
|---|---:|---:|---:|---:|
| `external_detached` | 2256.6 | 1858.1 | 0.823 | 1.270 |
| `external_handles` | 3007.5 | 2632.3 | 0.875 | 1.262 |
| `graph_reuse` | 2595.6 | 2083.6 | 0.803 | 0.928 |
| `idle_burst` | 3115.7 | 2610.8 | 0.838 | 1.124 |
| `local_saturated` | 842.0 | 701.1 | 0.833 | 0.706 |
| `nested_spawn` | 316.4 | 199.9 | 0.632 | 0.407 |
| `scope_recursive` | 767.1 | 686.9 | 0.895 | 0.750 |
| `steal_heavy` | 878.5 | 776.2 | 0.884 | 0.720 |

Below 1 means less elapsed time. The last column compares against the preserved
binary **before the entire topology rewrite**, not repository HEAD. Tiny external
submission still costs about 26–27% more than that baseline in this series.
With 400 iterations, external detached/handles are about 3% faster than before
this wake fix; small differences should not be treated as established wins.

Additional four-worker checks with 1, 2 and 8 shards cover shared domains and
empty domains. Detached time ratios after/before are 0.551, 0.791 and 0.825;
handles 0.835, 0.845 and 0.974; idle burst 0.417, 0.926 and 0.790. One-worker
results and the full matrix are in `final-summary.json`. This does not establish
scaling beyond four workers or multi-producer performance; the external suite
scenarios measured here use one producer. Multi-producer correctness remains
covered by the pool tests.

## PMU counters

Separate, alternating `perf stat` runs use three trials, 500 measured runs,
20 warmups and 4,096 tasks, four workers/shards, zero body iterations. Counters
are user-mode process totals across all threads, including warmup/setup, and
are not normalized as individual submit cost. All reported events ran 100% of
their enabled time in this capture. Median after/before:

| Event | Detached | Handles |
|---|---:|---:|
| `cycles:u` | 0.814 | 0.870 |
| `instructions:u` | 0.645 | 0.802 |
| `branches:u` | 0.640 | 0.816 |
| `branch-misses:u` | 0.937 | 0.945 |
| `cache-misses:u` | 0.805 | 0.919 |
| `L1-dcache-load-misses:u` | 0.748 | 0.898 |

The fall in instructions, cycles and cache misses supports the measured reduction
in wakeup work. It does not isolate the owner of each bouncing cache line.
`perf c2c` could not open AMD IBS events with the available permissions
(`perf_event_paranoid=2`); its error is preserved in `c2c-status.txt`. Kernel
settings were not changed.

## Remaining cost and rejected shortcuts

- `Pool::outstanding_` is already cache-line aligned. Annotate places about 76%
  of enqueue's local samples next to its locked increment, and 71% of execute's
  local samples next to its locked decrement. Together these regions account
  for roughly 12% of sampled cycles after this change. Sampling skid prevents
  exact instruction-cost attribution, but this is a concrete shared counter to
  investigate; padding alone cannot remove its true sharing.
- Ingress producer reservation uses relaxed CAS already, with release slot
  publication; consumer head reservation also uses relaxed CAS. Replacing these
  with plain stores would break MPMC ownership. Drain/polling and allocator/free
  are now larger remaining costs. They have not been causally isolated as the
  entire source of the remaining baseline regression.
- `select_shard` is about 1.7% of final samples, mostly around its round-robin
  `lock xadd` and modulus. A producer-local cursor/batch could be measured next,
  but was not mixed into this wakeup change.
- External submission does not invoke RNG. SplitMix is used by acquisition and
  stealing. No RNG change was needed.
- No `alignas(64)` blanket padding or new waiter state machine was added.
  Idle bits plus epochs retain the existing registration/notification lifecycle.
  Remote recruitment is preserved so occupied/empty home domains can use idle
  capacity elsewhere; the common small-pool path performs no remote-domain walk.

## Validation and reproduction

Final code passes system 16/16, mimalloc 16/16, tbbmalloc 15/15, ASan/UBSan 17/17
(leak detection disabled), and seven targeted TSan tests. The new tests cover
interleaved worker IDs, shared bitmap boundary words, partial domains spanning
words, empty-domain recruitment, publication before registration and 5,000 queue
publication/registration races. Progress checks fail if they need the five-second
wait timeout. Sanitizers and these tests are not exhaustive interleaving proofs
or weak-memory hardware validation.

Local, Git-ignored artifacts live under `out/benchmarks/wakeup-profile/`:
`compare.py`, `final-paired.jsonl`, `final-summary.json`, `final-manifest.json`
(binary/source hashes and commands), preserved `before` and `compact-probe`
binaries, `*.perf.data`, reports, annotated functions, `perf-commands.json`,
`*-stat.csv`, `perf-summary.json` and `tests-*.log`. The comparison script also
uses the preserved original binary from `out/benchmarks/memory-topology/`.

Representative profile command:

```sh
perf record -e cycles:u -F 999 -o wake.perf.data -- ./dagflow-runtime-suite \
  --scenario external_detached --workers 4 --tasks 4096 \
  --iterations 0 --warmup 20 --repeats 2000
perf report -i wake.perf.data --stdio --no-children
perf annotate -i wake.perf.data --stdio \
  --symbol 'dagflow::detail::ParkingLot::wake_one(unsigned int)'
```
