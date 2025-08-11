# ThreadPool

[![CMake](https://img.shields.io/badge/CMake-3.26+-blue.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![](https://tokei.rs/b1/github/cpp20120/ThreadPool)](https://github.com/cpp20120/ThreadPool).

Self written thread pool (was started when engine dev begins.

Mini-runtime for parallel tasks in C++20:

* Work-stealing pool (Chase–Lev decks, central MPMC queues).

* Lockfree MPMC (Michael–Scott) with hazard pointers (+ QSBR path).

* A local small_function for cheap closures.

* A lightweight DAG graph with cancellation, competition limits, and back-pressure.

* High-level API in the spirit of TBB: submit/then/when_all/parallel_for via TaskScope.

### Design 
* Scheduler: local decks (Chase–Lev) + "central" MPMC queues for external submissions; worker-first, then an attempt to steal from neighbors.

* MPMC: Michael–Scott + hazard pointers; there is a QSBR path (quiet state based reclamation) for queue tails in the pool.

* Synchronization:  memory_order (acquire/release/seq_cst where reconciliation is needed).

* Graph Security: Workers keep Core (shared_ptr) → no UAF, even if the TaskGraph was destroyed before wait().

By default, small_function<void(), **128** > in nodes. If you catch a Callable too large, increase the SSO (256) or pack the captures into the std::shared_ptr block.

For "heavy" stages, set concurrency > 1 in ScheduleOptions/NodeOptions.

Configure Config for CPU/NUMA; pin_threads=true is useful for cache stability.

In large pipelines, turn on back-pressure (capacity, Overflow) at the bottleneck stages.