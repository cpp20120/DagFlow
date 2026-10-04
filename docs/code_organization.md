# Code organization and API migration

The cleanup separates graph construction, graph execution, scheduling, and
storage. Graph definitions now have separate per-run counters, reusable sequential
runs, bounded admission, and iterative continuation execution. See
[the runtime architecture](how_it_works.md) for the execution contract.

Installed headers are split into `dagflow/` for the supported API and
`dagflow/detail/` for implementation dependencies. The detail headers remain
installable because public C++23 templates (`Pool`, `TaskGraph`, and scopes)
need their complete definitions at the consumer's compile site; they are not a
second supported API. Source-only headers would require a pimpl rewrite and
would make those templates impossible to instantiate from an installed package.

| Layer | Files | Responsibility |
| --- | --- | --- |
| Concurrent storage | `dagflow/detail/chase_lev_deque.hpp`, `dagflow/detail/ring_mpmc.hpp` | Bounded storage and publication; no scheduler waits or task ownership |
| Small utilities | `dagflow/detail/small_vector.hpp`, `dagflow/detail/small_function.hpp`, `dagflow/inplace_function.hpp` | Inline/SBO storage for successors and move-only callables |
| Completion | `dagflow/handle.hpp` | Observers, move-only completion credits, errors and dependency edges |
| Runtime storage | `dagflow/detail/runtime_memory.hpp`, `src/runtime_memory.cpp` | Unique object/array owners and dispatch to mimalloc, tbbmalloc or system allocation |
| Pool | `dagflow/thread_pool.hpp`, `src/thread_pool.cpp` | Task ownership, submission, workers and cooperative waiting |
| Scheduler | `dagflow/detail/scheduler.hpp`, `src/scheduler.cpp` | Contiguous local/shard storage, immutable membership, ingress and locality-first stealing |
| Parking | `dagflow/detail/parking_lot.hpp`, `src/parking_lot.cpp` | Compact idle bitmap/domain masks, paired fences, waiter epochs and CV handshake |
| Idle accounting | `dagflow/detail/idle_accounting.hpp`, `src/idle_accounting.cpp` | Single-writer counters, producer registration and external idle observation |
| Graph | `dagflow/task_graph.hpp`, `src/task_graph.cpp` | Node/edge construction, cycle validation, token dispatch and run accounting |
| Graph scope | `dagflow/graph_scope.hpp` | Reusable `emplace`, `then`, `when_all`, `parallel_for`, scoped graph waiting |
| Task scope | `dagflow/task_scope.hpp`, `src/task_scope.cpp` | One-shot dynamic spawn/join, descendant admission, cancellation and structured lifetime |
| Convenience include | `dagflow/dagflow.hpp` | Public pool, graph, and scope headers |

The API follows the explicit output-parameter convention of
[oneTBB concurrent queues](https://uxlfoundation.github.io/oneTBB/main/tbb_userguide/Concurrent_Queue_Classes.html)
and the graph-building `emplace` naming of
[Taskflow](https://taskflow.github.io/taskflow/classtf_1_1Taskflow.html).
This is naming and organization guidance, not API compatibility with either library.

## Concurrent containers

```cpp
dagflow::detail::ring_mpmc<int, 64> queue;
const bool accepted = queue.try_push(42);
int value = 0;
if (queue.try_pop(value)) {
  // Use value only on success.
}
```

Both containers expose `value_type`, `size_type`, `difference_type`, `capacity()`,
`try_push(value)`, and `try_pop(out)`. The deque additionally exposes
`try_steal(out)` and owner-only `free_capacity()`.

Only the deque owner may push or pop; multiple thieves may steal. The MPMC queue
supports multiple producers and consumers. Failed pops/steals leave the output
unchanged. A failed MPMC push leaves the input unchanged, including move-only
values. Element assignment must be nonthrowing once a slot has been reserved.

`empty()` is an observation, not a completion test or permission for a subsequent
pop. Containers deliberately provide no concurrent iteration or exact `size()`.
`try_steal` may fail because of contention. The MPMC head may be reserved but not
yet published; its `try_pop` can then fail even with other published entries.

Queue storage does not allocate during operations. User-defined element operations
may allocate; the scheduler uses task pointers. A paused MPMC producer can obstruct
consumers, so the complete pool is not a strictly lock-free or allocation-free API.

## Building versus submitting

`TaskGraph::emplace(f)` and `GraphScope::emplace(f)` only build nodes.
`TaskScope::spawn(f)` (also `submit`) schedules immediately, returns whether it
was accepted, and includes descendants in the scope completion. It does not
return a graph node or a per-task handle. See [the scope contract](task-scope-lifetime.md). `Pool::submit(f)`
schedules work immediately and returns a completion `Handle`.
`Pool::submit_detached(f)` schedules work without an individual completion counter.
The caller must keep borrowed resources alive until detached work finishes.
Pool destruction now drains accepted work; graphs/scopes and external callers
still must stop using the Pool before its storage is destroyed. See
[overflow and shutdown](pool-lifecycle.md).
`JobHandle` identifies a graph node; `Handle` tracks execution completion.

The scheduler has one acquisition path shared by workers and cooperative waiting:
local high/normal queues, the worker's central high/normal shard, then stealing
and other shards, with periodic service of shared ingress.
Acquisition, batch transfer, execution/completion, and parking have separate
functions. Pool tasks always belong to the pool.

`small_vector<T, N>` is a move-only vector with STL-style aliases, iterators,
element access, `emplace_back`, `push_back`, `pop_back`, `reserve`, and `clear`.
It uses inline storage until it spills, then allocates through `runtime_memory`.
Inline storage aligns to `alignof(T)`; heap storage aligns to
`max(DAGFLOW_CACHE_LINE_SIZE, alignof(T))`. Automatic growth
doubles capacity (up to `max_size()`); `reserve(n)` grows to exactly `n` elements.
`clear` retains allocated capacity, and moving a spilled vector transfers its
buffer without allocation. Allocation counts exclude allocations by `T` itself.

`small_function<Sig, N, Align>` and `inplace_function<Sig, N, Align>` are
move-only wrappers with one pointer to a static call/move/destroy table. The
defaults are 64 bytes and `alignof(std::max_align_t)`, with no cache-line padding.
Inline targets must fit both size and alignment and be nothrow-movable.
`small_function` otherwise allocates exactly `sizeof(F)` with `alignof(F)` through
`runtime_memory`, storing only the object pointer in its buffer. Heap moves steal
that pointer; both wrappers are always noexcept-movable. `inplace_function`
rejects non-inline targets at compile time. Target destructors must be noexcept.
The SBO buffer must be large/aligned enough to hold a pointer; the strict inline
wrapper also supports smaller buffers/alignments.

Both support `R(Args...)`, `operator bool`, `reset`, `emplace` and invocation.
`emplace` constructs a replacement before committing, preserving the old target
if allocation or construction throws. This adds one nonthrowing target move for
inline replacements. Empty invocation remains a precondition violation. There
is no copy, RTTI or target-inspection API. Use `dagflow/inplace_function.hpp` for the
strict wrapper; existing uses of `small_function` now permit spilling.

Graph building uses contiguous builder records with mutable successor lists and
options. `seal()` creates separate CSR `NodeDef[]` and `uint32_t Edge[]` arrays
through `runtime_memory`, then transfers callables without throwing. Only new
nodes need pending callable storage; this storage is released after seal.
Existing mutable callable state survives re-sealing. Compiled nodes contain
normalized token admission, lane counts and submission options.

`RunState` holds spans into the compiled arrays and graph-owned `NodeState[]`.
The latter grows on seal and is reset in place on each run; `reset()` retains
its capacity. `clear()` releases compiled/runtime arrays. Node/edge counts are
limited to `UINT32_MAX`; internal indices, predecessor and lane counters use
32 bits. Public node-ID inputs and token counts/cursors retain `size_t`.

## Migration

| Previous spelling or behavior | Current API |
| --- | --- |
| Deque `try_push_bottom(value)` | `try_push(value)` |
| Deque `pop_bottom(out)` | `try_pop(out)` |
| Deque `steal(out)` | `try_steal(out)` |
| MPMC optional-returning `pop()` | `bool try_pop(T& out)` |
| `SubmitOptions::skip_counter` | `pool.submit_detached(f, options)` |
| `SubmitOptions::owned` | Removed; internally allocated tasks are always pool-owned |
| Inline-only `small_function<Sig, N>` | Now SBO with spill; use `inplace_function<Sig, N>` to reject non-inline targets |
| `TaskGraph::add_node` | Prefer `emplace`; old spelling retained |
| `TaskGraph::set_tokens(...) const` | Now non-const: changing token counts invalidates compiled options |
| DAG-building `TaskScope` | Renamed to `GraphScope` in `dagflow/graph_scope.hpp`; `emplace`, `then`, `when_all`, `parallel_for`, `run`, `run_and_wait`, `clear` retain their graph semantics |
| Old `TaskScope::submit` returning `JobHandle` | Use `GraphScope::emplace` or `GraphScope::submit`; new `TaskScope::submit` immediately schedules and returns `bool` |
| Separate `ScheduleOptions` struct | Alias of `TaskGraph::NodeOptions` |
| Unused `task.hpp`, `qsbr_domain.hpp` | Removed; pool task storage is internal and bounded queues need no QSBR |
| Broken `TaskScope::for_each_index_ws` helper | Removed; use iterator-based `Pool::for_each_ws` or `GraphScope::parallel_for` |
| `Handle::Counter`, `Handle::get()` and construction from shared state | Removed; use `ready()`, `Pool::wait()` and `rethrow_if_failed()`; credit creation is internal |
| `Pool::memory_stats()` | Removed with the custom slab allocator; use the selected allocator's tooling for memory measurements |
| Manual compilation of only `src/thread_pool.cpp` | Also compile/link `src/scheduler.cpp`, `src/parking_lot.cpp`, `src/idle_accounting.cpp`, `src/task_graph.cpp`, `src/task_scope.cpp` and `src/runtime_memory.cpp`; CMake targets select and link the allocator |

`GraphScope::parallel_for` now creates one callable copy per chunk instead of
repeatedly moving from the same callable. Its callable must be copy-constructible.
`Pool::for_each_ws` now handles segmented random-access ranges such as `std::deque`
without assuming contiguous memory.
`Pool::combine` registers completion edges, including already completed and empty
handles, without scheduling waiting tasks. Errors propagate into the combined
handle; use `rethrow_if_failed()` after `Pool::wait()` to observe them.

## Validation and remaining work

CTest covers queue races/saturation, zero queue-storage allocations, container
lifetimes, completion errors/registration races, graph barriers and cancellation,
reusable graph scopes, dynamic child spawning after close, cancellation and
spawn/close races, cooperative/self joins, admission policies, allocation failures, scheduler fairness,
explicit credit transfer, payload teardown before readiness and cross-thread
allocation/free. `DAGFLOW_BUILD_RUNTIME_BENCH=ON` builds standalone
runtime benchmarks, including `dagflow-runtime-suite`; see the
[suite runner and methodology](benchmarks/runtime-suite.md). `DAGFLOW_BUILD_BENCH=ON` builds the oneTBB comparison benchmark.
See [measured results](runtime-benchmark-results.md).
`DAGFLOW_BUILD_FUNCTION_BENCH=ON` builds the callable storage/dispatch microbenchmark;
see [wrapper size and timing measurements](benchmarks/function-storage.md).

Use `-DDAGFLOW_ALLOCATOR=system` for sanitizer and allocation-failure checks.
The failure-injection target is only enabled for that backend because it intercepts
global `operator new`, not third-party allocation calls.
For ThreadSanitizer, build the test targets except
`dagflow_queue_allocation_tests` and `dagflow_runtime_failure_tests`; their global
`operator new/delete` overrides conflict with the Clang TSan runtime. Run CTest
with `-E 'queue_allocation|runtime_failure'`. ASan/UBSan can run the failure test;
LeakSanitizer may be unavailable under a tracing execution environment.

Graph definitions and CSR edges now use contiguous arrays, and node-state storage
is reused across runs. AoS/SoA alternatives, additional alignment and false-sharing
padding remain profiling work; this change does not select such layouts.
Completion state uses the selected allocator and separates observer references
from live credits; runtime code has no `shared_ptr`.
See [explicit ownership](explicit-ownership.md) and [scheduler topology](scheduler-topology.md).
`Config::worker_shards` selects logical groups; affinity now maps a worker hint
through that group. Rebuild consumers for the changed Config/Pool layout.
Caller participation and alternative queue/parking policies remain profiling work.
Public layout and completion state changed; rebuild library consumers together
with the library.
