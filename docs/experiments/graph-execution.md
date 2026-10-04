# Compiled graph execution

This pass follows the drain/wake experiment. The baseline already contains the
retained wake relay and scalar drain. The allocator, scheduler and queues are
identical between variants; allocation-backend experiments remain separate work.

## Retained structure

The existing builder/compiled split, CSR, contiguous NodeDef/NodeState arrays,
uint32_t topology counters, normalized execution options and graph-owned lifetime
are preserved. `seal()` additionally saves the source-node prefix from Kahn's
validation queue into an exact-sized root array. It costs one seal-time allocation
and four bytes per root. Roots are rebuilt after mutation, and this allocation
precedes callable transfer, so allocation failure preserves callable ownership.

Each run initializes only multi-predecessor arrival counters, then publishes the
compiled root list. The initialization scan is still O(nodes); this is not a
claim of O(roots) total startup. Each node's unique activator resets its cursor
and lane count before publishing any lane. That also handles repeated runs where
cancellation left a node unactivated or partially executed.

## Ownership and synchronization

- **One invocation:** the compiled lane count is necessarily one. Execute the
  callable directly, without a token cursor, claim CAS or token continuation.
  Cancellation is checked before the call and exceptions use the same error path.
- **One lane:** node completion needs no lane RMW because there is no competing
  lane. A tokenized node may still migrate through continuation publication and
  enter cooperative helping; its token claims retain the original CAS loop.
- **Several lanes:** retain bounded token CAS and the final acq_rel lane decrement.
  `fetch_add` is not a substitute: token limits include SIZE_MAX. Cancellation
  does not allow switching an already compiled multi-lane node into the exclusive
  path merely because fewer drivers are currently running.
- **One predecessor:** the sole incoming edge has one releaser; direct execution
  or queue publication carries its writes. No arrival RMW is necessary.
- **Several predecessors:** every edge contributes an arrival, including duplicate
  edges. The last acq_rel decrement gathers predecessor writes and activates the
  node once. Removing that join would break visibility, not just accounting.
- **Budget:** retain the 64 token/transition-step bound. Check for remaining tokens
  before yielding, so exact-budget completion does not publish an empty packet.
  With concurrent lanes a remaining-token probe may race another claim, which is
  harmless: the published continuation still owns the lane it eventually retires.
- **Lifetime:** the caller retains its completion credit across publication and
  error handling. A publication failure cancels and retires the reserved lane.
  Packet epilogues do not dereference graph state after their credit retires.

No new shared atomic, generation counter, runtime switch, allocator policy or
callable representation is introduced. Priority/affinity changes still prevent
direct successor bypass. Existing worker inline overflow remains a separate pool
limitation; this pass does not promise bounded stack use under queue saturation.

## Validation

Release: 28/28 tests. ASan/UBSan with leak checking: 28/28 tests. TSan: graph suite
passes, including visibility through fan-in and single-lane helping/continuation.
The allocation-failure suite passes in Release and ASan/UBSan; it cannot link with
this TSan runtime because both define global new/delete interceptors, so it is
not counted as TSan coverage.

New regression cases cover 1/63/64/65/127/128/129 tokens with one/four lanes,
repeated execution, mutable single-lane state while helping, duplicate edges,
fan-in visibility, alternating cancellation before activation, and changing the
root set on re-seal. A one-worker priority test verifies yielding from both a
token loop and a chain of zero-admission nodes. Worker-local allocation failure
at token 64 demonstrates
that a 64-token node finishes without a continuation, while a 65-token node
correctly reports failure of its required continuation. Existing tests cover
SIZE_MAX cancellation, zero admitted tokens, capacity, cycle rejection, failed
seal retry and destruction immediately after graph completion.

## Measurement method

`scripts/benchmark_graph_execution.py` compares frozen `sources/baseline` and
`sources/candidate` snapshots under `out/experiments/graph-execution`. Both use
the identical updated runtime suite, including its two new token scenarios.
Clang O3 with and without full LTO, static DagFlow and mimalloc are held fixed.
The runner builds all variants before timing, restricts each process to workers+1
distinct physical cores, randomizes case order and baseline/candidate order
inside each pair, and records source/binary hashes, commands and raw JSON.
Threads are not individually pinned and CPU frequency is not locked.

The matrix covers chain, fan-out/fan-in, independent roots, build+run, serial
tokens and concurrent tokens: 4096 payloads, 1/4 workers and 0/128 payload
iterations. Four additional cases check 64/65-node chains and serial tokens on
one worker. Each of the 28 cases runs in both profiles and both variants for five
independent rounds: 560 processes, ten warmups and 51 measured runs per process.
Every run checks outputs; token scenarios also check the invocation count.
Their benchmark-owned atomic ticket selects output slots and is included in the
timings, so these are not measurements of isolated token-claim instructions.

```sh
python3 scripts/benchmark_graph_execution.py out/experiments/graph-execution \
  --name another-run --rounds 5 --warmup 10 --repeats 51
```

The runner expects the snapshots and refuses to overwrite an existing result
directory. It does not modify the working tree or create a baseline from Git.

## Selection and final results

Two broader candidates were rejected. The first removed token CAS for all
single-lane nodes inside a shared loop; the second specialized the two token
loops. Both improved chains, but the first regressed the empty four-lane O3 case
by 17.5%, and the second regressed that case with LTO by 21.1%. These comparisons
do not isolate code layout from scheduling/contention effects. Their
[first report](graph-execution-data/comparison-report.md),
[second report](graph-execution-data/split-loops-report.md) and patches are
archived, with neither alternative retained in production.

The selected variant specializes only single-invocation nodes, preserving the
original bounded CAS loop for tokenized nodes. The compiled roots, activation-time
cursor initialization, single-lane completion, single-predecessor activation and
empty-continuation fix are retained. The benchmarked runtime differs from its
baseline only in `task_graph.hpp` and `task_graph.cpp`.

Final medians for 4096 empty payloads on four workers:

| Graph | O3 baseline → selected, µs | Change | Full LTO baseline → selected, µs | Change |
|---|---:|---:|---:|---:|
| Chain | 184.44 → 118.17 | -35.9% | 180.92 → 117.90 | -34.8% |
| Fan-out / fan-in | 625.36 → 574.87 | -8.1% | 597.06 → 568.99 | -4.7% |
| Independent roots / reuse | 1066.00 → 1043.47 | -2.1% | 1067.40 → 1048.73 | -1.7% |
| Build + run chain | 356.58 → 290.73 | -18.5% | 347.28 → 293.43 | -15.5% |
| Concurrent tokens | 219.28 → 213.09 | -2.8% | 351.98 → 344.49 | -2.1% |

One-worker empty chains improve 20.8% / 16.8%. The broad token-loop regression
does not recur in the final empty concurrent-token case, but this is **not an
across-the-board speedup**. With 128 payload iterations, the four-worker chain
regresses 10.9% with LTO (overlapping process ranges), and serial tokens regress
7.8% with LTO (non-overlapping process ranges in this run). The 65-token tiny
O3 case is also 12.3% slower, with overlapping ranges. Do not erase these results
or treat short-run noise as a proven explanation. Most other nontrivial-payload
changes are small; the retained change targets ordinary DAG node overhead.

The [full final table](graph-execution-data/final-report.md) preserves all 56
case/profile rows and their ranges. [CSV](graph-execution-data/final-summary.csv),
[manifest](graph-execution-data/final-manifest.json),
[retained runtime patch](graph-execution-data/candidate.patch), and test logs
are stored beside this document. Raw samples and commands for all three matrices
remain in `out/experiments/graph-execution/{comparison,split-loops,single-invocation}`.
Each matrix contains 560 processes; final results come only from
`single-invocation`, without pooling samples from discarded implementations.

The graph still allocates per-run completion/run state and published task
packets, and still stores erased node callables. Those costs, graph packet
specialization, AoS/SoA and cache-line placement require separate experiments.
This result closes the current execution-path pass, not all possible graph
optimization or the pool's existing overflow/shutdown work.

## Regression recheck

After the user reported possible interactive desktop activity during measurement,
all eleven positive-change case/profile points were repeated, together with the
two empty four-worker chain controls. No runtime changes or rebuilds were made:
the runner verified all four original binary SHA-256 hashes and reused the exact
original CPU masks. Each point used 15 randomly ordered baseline/candidate pairs,
30 warmups and 201 measured repetitions per process: 390 processes and 78,390
measured graph runs. No pairs or outliers were discarded.

| Previously suspicious case | Original change | Recheck change |
|---|---:|---:|
| Chain, 4 workers, 128 iterations, LTO | +10.9% | +3.1% |
| Serial tokens, 4 workers, 128 iterations, LTO | +7.8% | +1.7% |
| Serial tokens, 4 workers, 128 iterations, O3 | +1.1% | +4.1% |
| Serial tokens, 1 worker, zero iterations, O3 | +4.3% | -0.1% |
| Parallel-token configuration, 1 worker, zero iterations, O3 | +5.5% | -0.2% |
| Serial tokens, 1 worker, 65 tokens, O3 | +12.3% | -0.2% |

Changes above are ratios of medians of process medians, matching the original
table. The paired analysis adds useful context: the heavy LTO chain is slower
in 14/15 pairs, with a median paired change of +3.4% and a bootstrap 95% interval
of [+1.7%, +5.8%]. That remaining regression is reproducible in this series.
Heavy serial tokens on four workers with O3 are slower in 11/15 pairs, paired
median +4.8%, interval [+0.1%, +5.7%]; this also merits further investigation.
For heavy serial tokens with LTO the paired interval spans zero
([-1.4%, +6.2%]), so this recheck does not establish a consistent slowdown there.
Small positive effects remain in other rows; the full table retains them.

The empty-chain controls remain faster in all 15 pairs for each profile:
-35.1% without LTO and -34.7% with LTO by the ratio-of-medians measure. The
main chain benefit therefore survives this recheck. Much of the earlier large
regression magnitude does not; the remaining regressions cannot all be dismissed
as background noise. The experiment did not record window switching and cannot
identify Alt-Tab specifically as the cause. CPU frequency remains unlocked and
the process mask does not isolate its cores from other applications.

Intervals use 10,000 percentile bootstrap resamples of paired changes. They are
exploratory, without multiple-comparison correction; they do not prove absence
of smaller effects. Whole-process resource usage (including warmup/verification),
commands and all per-run timings are retained under
`out/experiments/graph-execution/regression-recheck/`.

[Full recheck table](graph-execution-data/recheck-report.md),
[CSV](graph-execution-data/recheck-summary.csv), and
[manifest](graph-execution-data/recheck-manifest.json) supplement, rather than
replace, the original results. Reproduce against unchanged recorded binaries:

```sh
python3 scripts/recheck_graph_execution.py \
  out/experiments/graph-execution/single-invocation \
  --output out/experiments/graph-execution/another-recheck
```
