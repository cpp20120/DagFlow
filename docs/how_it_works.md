### Architecture

How it works:

```mermaid
sequenceDiagram
    participant User
    participant TaskScope
    participant TaskGraph
    participant Pool
    participant Worker
    participant ChaseLevDeque as Worker Deque (Chase–Lev)
    participant CentralMPMC as Central Shard (MPMCQueue)
    participant HazardDomain

    Note over TaskGraph: token == one node execution<br/>max_concurrency limits parallel workers per node

    User->>TaskScope: submit(task, options)
    TaskScope->>TaskGraph: add_node(task, NodeOptions)
    TaskGraph->>TaskGraph: store node in nodes_

    User->>TaskScope: run()
    TaskScope->>TaskGraph: run()
    TaskGraph->>TaskGraph: seal() (cycle check)
    TaskGraph->>TaskGraph: reset() & prime tokens (sources)

    loop For each ready node token
        TaskGraph->>Pool: submit(node fun, SubmitOptions)
        alt submit called from worker thread
            Pool->>ChaseLevDeque: push_bottom(task) on that worker
        else submit called from external thread
            alt affinity set
                Pool->>CentralMPMC: push(task) to shard (affinity % shards)
            else no affinity
                Pool->>CentralMPMC: push(task) round-robin shard
            end
        end
    end

    par Worker scheduling loop
        Worker->>ChaseLevDeque: pop_bottom()
        alt Got local task
            Worker->>Worker: execute task (node token)
        else No local task
            Worker->>ChaseLevDeque: steal() from other worker
            alt Steal successful
                Worker->>Worker: execute stolen task
            else Steal failed
                Worker->>CentralMPMC: pop()
                alt Got task from central
                    Worker->>Worker: execute task
                else No tasks
                    Worker->>Worker: backoff (sleep/yield)
                    Worker->>HazardDomain: maybe_advance()  %% periodic epoch advance
                end
            end
        end
    and Node execution
        Worker->>TaskGraph: run node fn for one token
        alt fn throws
            TaskGraph->>TaskGraph: capture first exception & set cancel=true
        else fn ok
            TaskGraph->>TaskGraph: normal completion
        end
        TaskGraph->>TaskGraph: queued--, inflight-- (on finish)
        Worker->>TaskGraph: for each successor: preds_remain--
        alt successor became ready (preds_remain==0)
            TaskGraph->>TaskGraph: prime successor tokens
            TaskGraph->>Pool: submit(...) for successor tokens
        end
        TaskGraph->>Pool: complete_counter() (per token)
        alt inbox not empty AND inflight < max_concurrency AND !cancel
            TaskGraph->>Pool: reschedule worker for same node
        end
    end

    rect rgb(250,250,250)
    note over TaskGraph: Overflow policy on enqueue:<br/>Block = wait until space<br/>Drop = best-effort enqueue<br/>Fail = set error & cancel
    end

    note over CentralMPMC,HazardDomain: HP/QSBR are used inside MPMCQueue push/pop<br/>(acquire_thread_rec, protect, retire_qsbr)

    Pool->>TaskScope: Handle::Counter reaches zero
    TaskScope->>User: wait() completes (via Pool::wait(handle))

```

### Why not TBB
This runtime is not a full general-purpose task scheduling framework like Intel oneTBB.
It’s a focused, lightweight, single-run DAG executor designed for cases where you build a small or medium dependency graph and run it once (or a few times), rather than keeping a long-lived task arena alive.

**Main differences vs TBB-style runtimes:**

1. **Single-run DAG model**  
   - You explicitly build a graph (`TaskGraph` or via the higher-level `TaskScope`) with known dependencies.  
   - Once you call `run()`, the graph is *sealed* (cycle check, static indegrees calculated) and executed to completion.  
   - No implicit dynamic spawning of tasks during execution (although each node can submit more tokens of itself via `parallel_for` or rescheduling).

2. **Token-based execution model**  
   - Each node’s *unit of work* is a **token** (one execution of `fn`).  
   - Nodes can have `max_concurrency` limits and back-pressure via inbox `capacity` + `Overflow` policy (`Block`, `Drop`, `Fail`).  
   - This makes per-node load control and memory bounds *deterministic* — something TBB does not directly expose.

3. **Integrated back-pressure and overflow control**  
   - In TBB, tasks pile up in queues if producers outpace consumers; here you can choose to block, drop, or fail when inbox capacity is reached.  
   - This is critical in real-time-ish or bounded-memory pipelines.

4. **Explicit `ScheduleOptions` with affinity and priorities**  
   - You can pin a node’s tokens to a specific worker (or shard) and give them `High` vs `Normal` priority.  
   - Affinity is respected even for fan-out reschedules, unlike TBB which mostly relies on work stealing locality heuristics.

5. **Work-stealing pool as a standalone, minimal runtime**  
   - Under the hood is a Chase–Lev per-worker deque + central MPMC shards for external submissions.  
   - You can use the pool directly without TaskGraph, like a very lean replacement for `std::thread` + `std::async` — without heavy allocators or global state.

6. **Integrated memory reclamation for lock-free structures**  
   - Hazard Pointers + QSBR domain is part of the runtime.  
   - MPMC queues and other internals are safe for lock-free pushes/pops without external GC or RCU glue.

7. **Small-function and small-vector for zero-dependency, allocation-friendly core**  
   - No `std::function` allocations for tiny callables.  
   - Inline storage for small vectors without heap hits.

8. **Minimal ABI & zero global singletons**  
   - You own the `Pool` and `TaskScope`. No hidden thread arenas or global TLS registries (except hazard-domain TLS).  
   - Easy to embed in game engines, simulation loops, or tools where you need predictable teardown.
