# Graph allocation experiment

> **Historical workflow note (October 2026).** This report preserves its original
> benchmark commands and measurements. The former `scripts/benchmark_*.py`
> orchestration is retired; those commands are not part of the current build.
> For reproducible runs use [CMake benchmark campaigns](../benchmarks/campaigns.md).


## Frozen starting point

The retained scalar drain, wake relay and single-invocation graph execution were
saved before this experiment in `out/experiments/graph-allocations/checkpoint/`.
The checkpoint contains source, tests, documentation and four copied O3/full-LTO
benchmark binaries, verified against the preceding experiment's hashes. This is
a filesystem snapshot, not a Git commit of the mixed working tree. The complete
manifest is `out/experiments/graph-allocations/checkpoint-manifest.json`; its hash
and the previous reports are recorded in
[checkpoint.json](graph-allocation-data/checkpoint.json).

The earlier [graph regression recheck](graph-execution.md#regression-recheck)
remains part of the baseline: heavy-chain LTO and four-worker serial-token
regressions were not declared solved.

## Measurements before changing storage

New diagnostic-only counters record successful requests at the runtime allocator
boundary, requested bytes, deallocations, owning packet allocations/bytes,
RunState allocations and graph budget continuations. The runtime suite snapshots
them around measured work and final packet cleanup, excluding warmup and output
verification. Ordinary builds compile instrumentation away and emit null.

Baseline counts per warmed run of 4096 empty payloads:

| Shape | Allocations | Owning packets | RunState | Completion |
|---|---:|---:|---:|---:|
| Chain | 130 | 128 | 1 | 1 |
| Fan-out / fan-in | 4098 | 4096 | 1 | 1 |
| Independent roots | 4098 | 4096 | 1 | 1 |
| Serial tokens | 66 | 64 | 1 | 1 |

These counts match at one and four workers except concurrent-token scheduling,
where lane progress changes the number of continuations. The dominant request
count in wide graphs is packets, not RunState or completion. The first isolated
variant therefore reused RunState but was not advertised as a throughput win:
it saves one request per warmed run, with mixed/small wall-time changes in the
[separate matrix](graph-allocation-data/run-state-report.md).

Diagnostic owning graph packets are 48 bytes on this build; uninstrumented
packets are 40 bytes. The diagnostic allocating-thread field accounts for that
difference. Requested bytes do not measure allocator size-class rounding,
allocator metadata, cumulative live memory or RSS. Instrumented timings are
not used as performance results.

## Retained storage and lifetime

`seal()` reserves one `InitialTask` per node alongside reusable NodeState storage.
All new allocations precede callable transfer, preserving failed-seal ownership.
An initial root or ready successor borrows its slot for its first scheduled
driver. Bypassed nodes need no slot publication; a budget handoff to a successor
can use that successor's slot. No slot is reused twice within one run.

Additional lanes and same-node token continuations retain owning packets. An
earlier driver may still be returning through Pool's epilogue, or recursive
inline overflow/helping may execute the new driver before the old one returns.
Neither alternating two buffers nor a naive reusable per-node packet proves
that the old storage is unused. This experiment adds no freelist, lock, ABA
protocol or heuristic cache to work around that lifetime problem.

Pool already dispatches through erased invoke/release operations. Owning tasks
destroy their callable and allocation; an InitialTask releases its exclusive
borrow without destroying the array element. On success Pool moves the credit
out before invoking release, then finishes that credit. On rejected publication
release moves the child credit out and ends all slot access before retiring it;
the caller retains its own credit while cancelling and retiring the reserved
lane. Thus ready() implies that no old executor can access a graph packet slot.
Immediate rerun/reset/clear needs no pool-wide wait_idle barrier.

RunState storage is reused after readiness and reconstructed to reset spans,
cancellation, error and once_flag. A new CompletionState is allocated before
replacing the old run's result, so failure preserves that result. CompletionState
is not recycled through the graph: old Handles may outlive both run and graph,
and must retain their original readiness and error. `clear()` frees run and
packet storage; `reset()` preserves allocations while discarding the last result.

The allocator backend, routing policy, queues, scheduler, execution budget and
callable representation are unchanged. Ordinary Pool submissions still own their
packets. The cross-thread-free diagnostic skips borrowed slots because those
releases do not free an allocation.

## Allocation result and retained memory

With both changes, warmed chain/fan-out/independent-root runs perform **one
allocation: the 168-byte CompletionState**. There are zero owning task packets
and zero RunState allocations in these measured runs. A 4096-node chain still
has 127 budget handoffs and 128 scheduled drivers; this removes allocation cost,
not scheduling or fairness boundaries. Serial-token runs go from 66 allocations
to 64; 63 owning token continuations remain, plus completion. Concurrent-token
counts retain scheduling variance. First runs and cold producer registration
can allocate additional storage.

The tradeoff is **40 retained bytes per node** without diagnostics (48 with it),
or 160 KiB for 4096 nodes, even if many slots are bypassed. RunState grows from
88 to 104 bytes due to its additional span. No cache-line alignment is added.
Build+run diagnostic requests fall from 162 to 35, but requested bytes increase
from 2,840,600 to 3,031,080 because of the preallocated array. Fewer allocation
calls therefore do not imply less memory or faster one-shot construction.

[Raw allocation measurements](graph-allocation-data/allocation-counts.json)
include every command and result; diagnostic binary hashes are
[recorded separately](graph-allocation-data/diagnostic-binaries.json).

## Timing and validation

Each timing matrix uses the same suite in both variants, Clang O3 with and
without full LTO, static DagFlow and mimalloc. As in the previous graph pass:
28 cases, five paired independent rounds, ten warmups and 51 measured runs per
process, randomized order and process affinity to workers+1 physical cores.
The RunState-only and combined packet matrices each contain 560 processes.
Counters run separately; builds and sanitizer tests finish before timing starts.

Combined variant, 4096 empty payloads:

| Shape | Workers | O3 change | Full LTO change |
|---|---:|---:|---:|
| Chain | 1 | +6.8% | +2.0% |
| Chain | 4 | -8.1% | -8.3% |
| Fan-out / fan-in | 1 | -14.1% | -14.8% |
| Fan-out / fan-in | 4 | -17.2% | -16.8% |
| Independent roots | 1 | -35.7% | -38.2% |
| Independent roots | 4 | -0.9% | -7.6% |
| Build + run chain | 1 | +6.6% | +1.1% |

Negative change means less elapsed time. With 128 payload iterations on four
workers, fan-out/fan-in improves 13.1% / 13.3% and independent roots improve
21.4% / 18.3%. Tokenized nodes show mixed results and retain most allocations.
This is not a universal speedup: the [complete table](graph-allocation-data/initial-packets-report.md)
includes regressions, especially short noisy cases, and all process ranges.

Release and ASan/UBSan with leak checking pass 28/28 tests. TSan graph and
ownership suites pass 2/2; global-new failure injection is excluded from TSan
because of its conflicting interceptors. Added coverage repeatedly reuses 1024
root slots immediately after ready(), retains old Handles through reruns/clear,
and preserves an old exception after a later successful run. Failed seal retry,
cooperative destruction, cancellation, additional lanes, token continuations and
priority progress are covered by the existing graph suites. Publication failure
now includes aligned producer-registration failure after a prepared slot obtains
its credit, instead of assuming that every root allocates a packet.

## Repeat check of regressions

All 21 positive-change case/profile points plus the two four-worker empty-chain
controls were rechecked on the exact same binary hashes and CPU masks: 15 pairs
per point, 30 warmups and 201 measured repeats per process, for 690 processes.
There was no outlier removal. The [full recheck](graph-allocation-data/recheck-report.md)
retains paired medians and exploratory bootstrap intervals.

The one-worker empty-chain O3 change moves from +6.8% to -1.1%, one-worker
build+run from +6.6% to -0.4%, and the tiny 64-token O3 case from +49.5% to -1.8%.
The chain benefit at four workers repeats at -7.9% without LTO and -6.9% with LTO,
with the candidate faster in every pair in both profiles.

Small tokenized-node regressions remain: empty serial tokens at four workers/O3
are +1.3% by ratio of process medians, with a paired interval [+0.6%, +2.4%];
empty concurrent tokens at four workers/LTO are +1.8%, interval [+0.3%, +2.1%].
Heavy serial tokens with LTO are +2.1%, with a paired interval whose lower end
is close to zero. Heavy-chain LTO is +1.8% with an interval spanning zero.
These are results relative to this allocation experiment's baseline; they do
not erase the previous graph-execution regressions relative to its older
baseline. Intervals are exploratory and not corrected for multiple comparisons.

The combined change is retained for the substantial allocation reduction and
wide-graph gains, with the 40-byte-per-node memory cost explicitly accepted in
this implementation. It is not claimed to accelerate every workload. Further
packet pooling for token continuations, CompletionState coallocation, or backend
changes need separate lifetime designs and measurements.

## Reproduction and artifacts

```sh
python3 scripts/benchmark_graph_execution.py out/experiments/graph-allocations \
  --name another-packet-comparison
python3 scripts/measure_graph_allocations.py \
  --binary out/experiments/graph-allocations/build/diagnostic-candidate/dagflow-runtime-suite \
  --output out/experiments/graph-allocations/another-count-pass
```

The timing runner expects frozen `sources/baseline` and `sources/candidate`.
Archived `sources/run_state` and `sources/initial_packets` preserve the two tested
implementations. [RunState-only patch](graph-allocation-data/run_state.patch),
[combined patch](graph-allocation-data/initial_packets.patch), manifests, summary
CSV/JSON, and validation logs are archived next to this document. Full raw timing
JSON and command logs remain under `out/experiments/graph-allocations/`.

After this measurement, [shared overflow](../pool-lifecycle.md) added an 8-byte
intrusive link to the packet prefix. Current non-diagnostic InitialTask is 48 B
(192 KiB / 4096 nodes); the 40 B measurements above describe the frozen allocation
experiment. Shared overflow no longer invokes a driver inline, but an earlier
driver may still be in its epilogue while another worker executes its successor.
