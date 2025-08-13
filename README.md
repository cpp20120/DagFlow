# ThreadPool

[![CMake](https://img.shields.io/badge/CMake-3.26+-blue.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![](https://tokei.rs/b1/github/cpp20120/ThreadPool)](https://github.com/cpp20120/ThreadPool).

Self written thread pool (was started when engine dev begins.

A self-contained, minimal runtime for parallel task execution in C++20.  
Designed for workloads where you know the dependency graph in advance, need predictable scheduling, and want full control over affinity, priorities, and back-pressure — without the complexity of a full TBB.

Mini-runtime for parallel tasks in C++20:

* Work-stealing pool (Chase–Lev decks, central MPMC queues).

* Lockfree MPMC (Michael–Scott) with hazard pointers (+ QSBR path).

* A local small_function for cheap closures.

* A lightweight DAG graph with cancellation, competition limits, and back-pressure.

* High-level API in the spirit of TBB: submit/then/when_all/parallel_for via TaskScope.

### [Design](https://github.com/cpp20120/ThreadPool/blob/main/docs/how_it_works.md) 
* Scheduler: local deques (Chase–Lev) + "central" MPMC queues for external submissions; worker-first, then an attempt to steal from neighbors.

* MPMC: Michael–Scott + hazard pointers; there is a QSBR path (quiet state based reclamation) for queue tails in the pool.

* Synchronization:  memory_order (acquire/release/seq_cst where reconciliation is needed).

* Graph Security: Workers keep Core (shared_ptr) → no UAF, even if the TaskGraph was destroyed before wait().

By default, small_function<void(), **128** > in nodes. If you catch a Callable too large, increase the SSO (256) or pack the captures into the std::shared_ptr block.

For "heavy" stages, set concurrency > 1 in ScheduleOptions/NodeOptions.

Configure Config for CPU/NUMA; pin_threads=true is useful for cache stability.

In large pipelines, turn on back-pressure (capacity, Overflow) at the bottleneck stages.

### Benchmarks
CPU: Ryzen 7 6800H
OS: Windows 11
Build Mode: Release
Compiler: MSVCx64 19.44.35214 
Config: pin_threads = false

| Benchmark                              | Runs | Mean Time  | Min Time  | Max Time  | Cost of 1 task +-                  |
|----------------------------------------|------|------------|-----------|-----------|------------------------------------|
| **Dependent chain (1000 tasks)**       | 5    | 0.00579 s  | 0.00491 s | 0.00765 s | ~3–4 μs per node                   |
| **Independent tasks (1000)**           | 5    | 1.5283  s  | 1.41794 s | 1.64484 s | —                                  |
| **Independent batched (1000, batch=10)**| 5    | 1.41718 s  | 1.39467 s | 1.44458 s | ~7–20% faster, than without batch |
| **Parallel_for (1e6 elements)**        | 5    | 0.54729 s  | 0.53645 s | 0.57182 s | —                                  |
| **Workflow (width=10, depth=5)**       | 5    | 0.00161 s  | 0.00147 s | 0.00179 s | ~30–35 μs for all DAG              |
| **Noop tasks (1 000 000)**              | 5    | 4.411 s*   | 4.37711 s | 4.48551 s | ~4.7–4.8 μs per task              |


Compare with TBB

| Benchmark           |    Problem size | Your TP mean (s) | TBB mean (s) | Speedup (TP / TBB) |     TP throughput |      TBB throughput |
| ------------------- | --------------: | ---------------: | -----------: | -----------------: | ----------------: | ------------------: |
| Dependent chain     |      1000 tasks |          0.00579 |   0.00288156 |          **2.01×** | \~172,712 tasks/s |   \~347,034 tasks/s |
| Independent tasks   |      1000 tasks |          1.52830 |      1.25051 |          **1.22×** |   \~654.3 tasks/s |     \~799.7 tasks/s |
| Independent batched | 1000 (batch=10) |          1.41718 |      1.30121 |          **1.09×** |   \~705.6 tasks/s |     \~768.5 tasks/s |
| parallel\_for       | 1,000,000 elems |          0.54729 |    0.0268362 |         **20.39×** |   \~1.83M elems/s |    \~37.26M elems/s |
| Workflow (w=10,d=5) |  \~50 stage ops |          0.00161 |   0.00029248 |          **5.50×** |                 — |                   — |
| Noop tasks          | 1,000,000 tasks |            4.681 |     0.225568 |         **20.75×** | \~213,630 tasks/s | \~4,433,253 tasks/s |
