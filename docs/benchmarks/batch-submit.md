# Explicit batch submission benchmark

Measured 2026-09-30 on Linux x86_64 with Clang 22.1.8, static mimalloc,
Release binaries, no frequency lock, and no per-thread pinning. The process was
restricted to at most `workers + 1` distinct physical-core representatives.
Seven paired trials used ten warmups and 101 measured repetitions. Run medians
are compared pairwise; all checksums matched. Raw JSONL, commands, manifests and
the binaries are in `out/benchmarks/batch-submit/comparison/`.

The preserved hashes are `before = 37b3b2225f5345867b70c9c9f4027df2cfa715e866a5dd420a78870274c5d1e7`
and `after = 72a0cf292f725c14cf9d6d11973f68d6f022f8422a12bbbe0a47638b8d56908c`;
the exact commands and build provenance are recorded in `manifest.json`.

The `before` binary is the scalar detached-submit implementation immediately
before this experiment. The `after` binary contains the explicit
`submit_batch_detached` API, while `--submit-batch 0` uses the scalar path. The
batch path still allocates and publishes each task separately; it groups shard
selection and the normal wake decision, and places each group in one shard.

## Throughput

Ratios are measured median divided by the `before` scalar median; below 1 means
less elapsed time. The 4,096-task, zero-iteration case isolates scheduler and
submission overhead reasonably well. Four hundred iterations makes packet and
queue overhead a smaller part of the run.

| Workers | Iterations | Scalar after | Batch 1 | Batch 4 | Batch 16 | Batch 64 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 1.068 | 2.628 | 1.005 | 0.567 | 0.532 |
| 1 | 400 | 0.996 | 1.000 | 0.999 | 1.001 | 0.998 |
| 4 | 0 | 0.972 | 1.991 | 0.249 | 0.184 | 0.158 |
| 4 | 400 | 1.000 | 0.990 | 1.000 | 1.000 | 1.425 |

The batch 1 result is intentionally included as a control for API/staging cost;
it is slower because it still pays the batch preparation path without amortizing
anything. Batch 16/64 reduces the one-worker empty-task time by about 43–47% and
the four-worker empty-task time by about 75–84% against the new scalar path. The
four-worker result also benefits from sending each group to one shard instead of
round-robin selecting a shard for every task, so it is not a pure wakeup-only
measurement. That placement is part of the API contract and must be considered
when choosing it.

With 400 loop iterations, batch submission is within measurement noise for batch
4/16 and one worker. Batch 64 is 42% slower at four workers in this run, which is
consistent with a group becoming too large for the available queue/worker
interleaving. The API therefore remains opt-in and has no effect on scalar
`submit` or `submit_detached` callers.

## Latency and profile

For a one-task idle burst, batch sizes 4/16/64 did not produce a stable latency
gain: four-worker latency p50 was approximately 8.33, 8.44 and 8.50 µs versus
8.42 µs for the old scalar binary. A single task has no amortizable publication
work, so the batch API is not intended for this case.

A separate 2,000-repetition `perf record` on the scalar four-worker,
zero-iteration workload sampled approximately 24% in mimalloc free, 15.5% in
central queue drain, 7.1% in central push, 5.3% in `wake_one`, and 3.5% in shard
selection. This supports batching as a useful experiment, but does not prove
that any one of those costs explains the measured improvement.

## Correctness and limits

The new batch tests cover move-only callables, empty and partial spans, child
submission/cooperative waits, worker overflow, cross-pool submission, concurrent
producers, empty-domain recruitment, queue backpressure, long worker backoff,
packet and callable allocation failure, and detached task exceptions. System
CTest passes 19/19; the focused TSan set passes 3/3; the focused ASan/UBSan set
passes 3/3. The full batch comparison is single-producer for the runtime suite;
the correctness test has four concurrent producers.

Batch submission has a basic exception guarantee. Earlier groups can already be
running when preparation or publication of a later group throws, and moved input
callables can be left moved-from. No hidden producer buffer or second completion
protocol exists. See [the API contract](../batch-submit.md).
