# Explicit detached batch submission

`Pool::submit_batch_detached(std::span<F>, SubmitOptions = {})` consumes a span
of callable objects by move. Each callable executes as an independent detached
task. There is no per-task handle or shared batch completion state; exceptions
from task bodies follow `submit_detached` policy and are discarded. Capture an
error sink if needed, and keep borrowed data alive until accepted work finishes.

```cpp
std::vector<decltype(make_job(0))> jobs;
for (unsigned i = 0; i < 16; ++i) jobs.push_back(make_job(i));
pool.submit_batch_detached(std::span{jobs});
pool.wait_idle();  // External callers only; includes descendants and teardown.
```

The callable type may be move-only; a span of `small_function<void()>` can hold
heterogeneous jobs. An empty span does nothing. On success all input jobs have
been submitted (and some may already have completed); nothing is retained in a
producer-side buffer awaiting another call. Return does not imply completion.
Submission does not promise execution order or simultaneous visibility of the
whole batch. Borrowed data must survive accepted work, including the graceful
pool shutdown drain.

## External path

For each group of at most 64 jobs:

1. Prepare independent task packets through `runtime_memory`. A stack array of
   owning pointers holds them until publication; it needs no heap allocation.
2. Select one shard, respecting the existing affinity-to-home-shard mapping.
3. For each packet, count publication in the producer lane, then push into the
   existing MPMC queue. Accounting and queue reservation remain per task.
4. Execute one normal parking wake decision after the group's publication.

Longer spans are split into groups, with fresh routing and wake decisions.
Queue saturation still applies backpressure. The producer wakes a worker before
waiting/retrying, so a full queue cannot postpone notification until batch end.
The existing publisher fence runs after queue publications and pairs with idle
registration/final acquisition on workers. Recruitment by workers draining into
local queues remains active; other workers can steal the published tasks.

This changes task placement as well as wake frequency: adjacent jobs enter the
same shard and can be drained together. A measured gain cannot be attributed
solely to fewer wakeup fences. The implementation does not reserve multiple MPMC
slots with one CAS and does not recycle task storage through a new allocator.

## Workers and failure

Calls from a worker of this pool use ordinary per-task `enqueue`, preserving
Spawn/Enqueue routing, cooperative waits and shared overflow behavior. They do
not gain wake coalescing in this version. A worker of another pool is an external
producer with respect to the target pool.

Callable construction/packet allocation finishes for a group before any of its
tasks are published. If preparation throws, that group's prepared packets are
destroyed; earlier groups may already be running. Input callables can be moved
from even when they were not accepted. An accounting/registration failure during
publication can leave an accepted prefix; it wakes any tasks already published.
This is a basic exception guarantee, not transactional submission or an automatic
join on failure. The caller must also preserve borrowed data on the error path.

Each accepted task uses the same physical retirement path as scalar submission:
callable destruction, packet destruction, completion-credit finish, then the
executor lane's retirement count. No batch sentinel or second completion
protocol is added. Concurrent `wait_idle()` does not wait for jobs still being
prepared but not yet accepted, just as it does not close scalar admission.

## Checks and measurements

Tests cover move-only jobs, empty/partial groups, children and cooperative waits,
worker overflow, cross-pool calls, concurrent producers, external backpressure,
long-backoff wake progress, construction failure after an accepted group,
packet-allocation failures and task-body exceptions.

See [the benchmark report](benchmarks/batch-submit.md) for results and limits.

External admission is reserved once per prepared group. `close()` can reject a
later group with `logic_error` after an earlier group was accepted. Accepted
groups complete publication even under backpressure; shutdown waits for them
and their task epilogues. See [pool lifecycle](pool-lifecycle.md).
