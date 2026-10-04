# Scalar noop hot-path investigation

Measured target: current DagFlow runtime after topology, wakeup, single-writer accounting, and explicit batch-submission work.

This document is a **diagnostic plan**, not a performance claim. The goal is to explain and, if possible, reduce the cost of the scalar tiny-task path without weakening the current lifetime, publication, completion, parking, or `wait_idle()` contracts.

## Motivation

The historical benchmark recorded approximately:

- `Noop tasks (1,000,000)`: **0.606 s mean** (~1.65 M tasks/s).

Running the old benchmark shape against the current runtime produced roughly:

- printed noop runs: **~1.19–1.21 s**;
- outer summary: **~1.30 s**.

The old harness therefore has at least two timing boundaries, and the historical table must not be treated as directly comparable until the measurement boundary is identified.

The modern runtime suite also shows that explicit batching can reduce zero-body external-submission time dramatically. This means the scalar regression may be dominated by per-task scheduler coordination rather than task execution itself.

## Questions

1. Is the historical `0.606 s` number measuring the same interval as the current legacy-benchmark summary?
2. What is the current steady-state scalar noop cost in the modern benchmark harness?
3. How much of that cost is attributable to:
   - packet allocation/free;
   - external ingress push/drain;
   - shard selection;
   - parking/wakeup;
   - pool accounting;
   - completion machinery;
   - benchmark/harness overhead?
4. Does explicit batch submission recover or exceed the historical throughput for the same million-noop workload?
5. After batching and accounting changes, what is the **new** profile? Old `perf.data` is not evidence for the current bottleneck.
6. Which candidate changes remove whole operations from the scalar path, rather than merely shortening address calculation or instruction sequences?

## Non-goals

- No hidden TLS producer buffer in scalar `submit`/`submit_detached`.
- No weakening of immediate scalar publication/backpressure semantics merely to improve a benchmark.
- No performance thresholds in correctness tests.
- No claim that a one-worker or four-worker result generalizes to NUMA or multi-socket systems.
- No reuse of historical benchmark numbers as a current ranking unless the timing boundary and workload semantics are reproduced.

## Baselines to preserve

Preserve binaries and source hashes before every experiment.

At minimum:

1. **Current scalar baseline**
   - current topology;
   - compact-bitmap wakeup;
   - single-writer accounting;
   - no rejected TLS worker-lane pointer cache;
   - explicit batch API present but disabled (`--submit-batch 0`).

2. **Current explicit batch**
   - same binary/configuration;
   - batch sizes `1, 4, 16, 64`.

3. **Legacy benchmark executable**
   - run only as a historical diagnostic;
   - record both its per-run printed timing and its outer summary separately.

If an actual preserved historical binary exists, keep it as a separate baseline. Do not substitute repository HEAD or reconstruct an old result from the current benchmark harness.

## Environment

Record for every comparison:

- CPU and kernel;
- compiler and exact version;
- allocator backend;
- static/shared build;
- Release flags, LTO/PGO/native tuning;
- worker count;
- shard count and membership;
- pinning policy;
- process CPU-set restriction;
- AC/battery state;
- governor/EPP/frequency policy if known;
- binary and source hashes.

The primary comparison should use the same process restriction as the recent focused runs: at most `workers + 1` distinct physical-core representatives. This is a CPU-set restriction, **not** per-thread placement or a frequency lock.

## Benchmark matrix

### A. Scalar tiny-task baseline

Use the modern runtime suite.

For each worker count:

- workers: `1, 4`;
- tasks: `4096, 65536, 1000000`;
- body iterations: `0, 400`;
- scalar submission only;
- scenarios:
  - `external_detached`;
  - `external_handles`;
  - `local_saturated`;
  - `idle_burst`;
  - `graph_reuse`;
  - `nested_spawn`.

The million-task point is primarily for `external_detached` and `local_saturated`; do not make every scenario unnecessarily expensive.

Record:

- whole-run elapsed time;
- payload tasks/s;
- checksums;
- latency only in a separate run.

### B. Explicit batch control

For `external_detached`:

- workers: `1, 4`;
- tasks: `4096, 65536, 1000000`;
- body iterations: `0, 400`;
- batch sizes: `1, 4, 16, 64`.

Purpose:

- `batch=1` measures API/staging overhead with no amortization;
- `4/16/64` show how much per-task scheduler coordination can be amortized;
- the one-million noop point answers whether the new batch API recovers the old scalar-noop throughput.

Do not infer the one-million result from the 4096-task ratio.

### C. Legacy-harness boundary check

Run the current runtime through the old benchmark and capture:

- every printed per-run timing;
- the outer `Mean/Min/Max`;
- the exact code region timed by each.

The current output already shows that noop per-run timing (~1.2 s) differs from the outer summary (~1.3 s). Determine which boundary produced the historical `0.606 s` table before using it as a regression factor.

## Measurement protocol

For focused comparisons:

- alternate before/after order;
- deterministic point ordering;
- warm up before measured repetitions;
- use paired trials;
- retain every raw run;
- remove no outliers from the primary report;
- report:
  - median of run medians;
  - paired ratios;
  - disagreement between aggregate methods when present.

For large one-million-task points, choose enough repetitions to stabilize the median without turning the experiment into a thermal-duration test. Record the exact count rather than silently changing the protocol.

Small differences should not be called wins without a stable sign across paired trials.

## Re-profile after each architectural change

For scalar four-worker zero-body `external_detached`, capture a fresh profile after the accounting and batch work.

At minimum:

```text
perf stat:
  cycles:u
  instructions:u
  branches:u
  branch-misses:u
  cache-misses:u
  L1-dcache-load-misses:u
```

And:

```text
perf record
perf report
```

If available, add `perf c2c` for cache-line ownership attribution.

Do not reuse the earlier profile percentages as current attribution.

## Current hypotheses

The earlier scalar profile reported approximately:

- mimalloc free: ~24%;
- central queue drain: ~15.5%;
- central push: ~7.1%;
- `wake_one`: ~5.3%;
- shard selection: ~3.5%.

These are **historical hypotheses for the next profile**, not current truth.

### H1 — packet allocation/free dominates

Current scalar task lifetime:

```text
producer allocates Task
    -> publication / queueing
    -> worker executes
    -> callable + packet destroyed
    -> worker frees Task
```

Experiment only if fresh profiling still shows allocator/free as a major share.

Candidate prototype:

- fixed-size `Task` recycler;
- preferably shard/worker-local retirement ownership;
- no general-purpose DagFlow slab allocator;
- no new per-task metadata unless measured necessary.

Questions:

- can reuse avoid a new shared freelist CAS hotspot?
- what happens for external producer -> remote worker retirement?
- does a cache reduce allocator cost without increasing cache footprint enough to lose elsewhere?
- how are shutdown, failure injection, and partial startup handled?

Success criterion: stable whole-run improvement, not merely fewer allocator samples.

### H2 — external ingress is too general for tiny scalar tasks

If fresh profiling still shows central push/drain as a major share, prototype a per-shard ingress design rather than only micro-optimizing the existing MPMC operations.

Candidate shape:

```text
external producers
      -> per-shard MPSC inbox
      -> shard owner drains
      -> local Chase-Lev work
      -> thieves steal normally
```

This is an architectural experiment.

Must preserve or explicitly redefine:

- backpressure;
- wakeup on a sleeping shard;
- fairness between ingress and local work;
- multi-worker shard ownership;
- stop/shutdown;
- overflow behavior;
- cross-shard and affinity semantics;
- worker cooperative progress.

Measure before discussing replacement of the current ingress.

### H3 — wake/shard selection remains per-task tax

Batch results already show that grouping:

- shard selection;
- wake decision;
- same-shard placement

can drastically reduce zero-body overhead.

Diagnostic variants may separate:

1. same-shard grouping with per-task wake;
2. round-robin placement with one wake per group;
3. full current batch behavior.

This decomposes placement benefit from wake amortization and their interaction.

### H4 — accounting publication can be bulked for explicit batches

This is **not** a scalar-path optimization, but it may further reduce explicit-batch overhead.

Current batch preparation finishes all throwing packet/callable construction before publication of a group. A candidate protocol is:

```text
fallible preparation
    -> register/check producer lane
    -> reserve/check publication count for N
    -> one bulk publication store
    -> non-throwing queue publication of N packets
    -> wake
```

Required invariant:

- accounting may become visible before queue visibility;
- queue-visible work must never exist before its accounting publication;
- after bulk accounting commit, every counted packet must eventually become retire-able or be compensated by an explicit accounting protocol.

Do not add rollback accounting merely to make the experiment convenient unless the queue path cannot provide a non-throwing post-commit phase.

### H5 — worker retirement batching

Do **not** implement unless a fresh profile demonstrates a meaningful retirement-store cost.

Worker retirement is already single-writer and local. Delaying retirement stores would create a flush protocol around:

- idle transition;
- parking;
- `wait_idle()`;
- worker exit;
- shutdown;
- nested/cooperative waits.

The semantic cost is high; the current single-writer release store must first prove expensive enough to justify it.

## Correctness gates for any candidate

Every experiment must retain:

- publication before queue visibility;
- retirement only after callable destruction and completion propagation;
- stealing requires no accounting transfer;
- `wait_idle()` cannot return while accepted physical packets or causally published children remain;
- no lost wakeup;
- queue backpressure still makes progress;
- worker inline/overflow semantics remain unchanged unless the experiment explicitly targets them;
- exceptions cannot strand accounting;
- pool destruction and partial thread startup clean up correctly;
- producer registration lifetime remains valid;
- cross-pool submission remains correct.

Run, as applicable:

- system configured suite;
- mimalloc suite;
- tbbmalloc suite;
- ASan + UBSan;
- targeted TSan;
- allocation-failure injection;
- batch partial/failure cases;
- long-backoff/lost-wakeup cases;
- cross-pool tests;
- multi-producer accounting tests.

## Decision rule

Keep a change only if it satisfies both:

1. **Semantic cost is justified**: the new lifetime/ownership/protocol state is no more complex than the measured benefit warrants.
2. **Measured benefit is established**: the relevant workload improves consistently under paired runs.

A prettier `objdump`, fewer loads, fewer saved registers, or a lower instruction count is not sufficient.

Negative experiments are retained as artifacts with a short note explaining why they were rejected, so the same idea is not reimplemented later without new evidence.

## Immediate next run

1. Re-run modern scalar `external_detached` at `4096`, `65536`, and `1,000,000` zero-body tasks for `1` and `4` workers.
2. Run the same points with batch `1/4/16/64`.
3. Capture a fresh four-worker scalar zero-body `perf stat` + `perf record`.
4. Identify the exact timing boundary behind the legacy noop summary.
5. Only then choose between:
   - packet-recycler prototype;
   - ingress redesign prototype;
   - batch-accounting bulk publication;
   - doing nothing to scalar noop and treating explicit batch as the throughput API.

