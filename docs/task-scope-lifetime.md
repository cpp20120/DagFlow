# TaskScope: admission, completion and lifetime

`TaskScope` is a one-shot dynamic task group. The old reusable DAG builder is
`GraphScope` (`dagflow/graph_scope.hpp`). Its graph-building methods retain their behavior.
Include `dagflow/task_scope.hpp` for dynamic work, or `dagflow/dagflow.hpp` for both APIs.

```cpp
dagflow::Pool pool;
dagflow::TaskScope scope(pool);
scope.spawn([](dagflow::TaskScope::Context& ctx) {
  ctx.spawn([] { /* descendant */ });
});
scope.join();
```

The invariant is: every potential continuation owns a live completion credit.
Only a live credit can fork another; the last retirement publishes completion
exactly once. The counter never reopens after zero. A scope cannot be reset or
reused; construct another scope for another operation.

## Admission and API

| State | External `scope.spawn` | `ctx.spawn` from a live callback | Completion |
| --- | --- | --- | --- |
| Open | Accepted | Accepted | Held open by admission sentinel |
| Closed | Rejected | Accepted | Waits for publishers, callbacks and descendants |
| Cancelled / failed | Rejected | Rejected | Drains accepted work and its captures |
| Complete | Rejected | No valid context remains | Ready forever |

- `spawn(f, SubmitOptions{})` schedules immediately; `submit` is an alias.
  The callback takes `Context&` or no arguments. `true` means admission succeeded,
  not that the body will execute: cancellation may skip it. `false` means rejected
  without constructing or moving the supplied callable into runtime storage.
  Argument expressions are evaluated by C++ before the call, as usual.
- `close()` closes external admission and returns the aggregate `Handle`.
  It does not wait. It is idempotent; existing children may keep spawning children.
- `wait()` closes and waits without rethrowing task errors. `join()` additionally
  rethrows the first captured error. Both may be repeated.
- `cancel()` closes both admission paths. Queued callbacks check cancellation
  before user invocation; callbacks that passed that check may still run.
  Running callbacks can poll `Context::cancelled()` and call `Context::cancel()`.
  Cancellation alone is successful completion, not an exception.
- `completion()` returns an observer. Waiting on that observer while admission
  remains open cannot finish until someone closes or cancels the scope.
- Destruction closes and joins without propagating task errors, including during
  stack unwinding. It does not automatically cancel outstanding work.

External admission uses an atomic word containing closed/cancelled bits and a
count of publishers currently forking the root credit. A successful reservation
CAS orders external spawn against close/cancel. The first closer retires the
root sentinel if there are no reservations; otherwise the last reserving
publisher retires it. Closing never waits for publishers. The reservation count
protects root access only; completion credits still protect all accepted work.

Child admission reads the cancellation bit and forks its live parent credit,
without touching the root or taking a mutex. That read is its admission point:
a concurrent cancel can follow an accepted reservation. Already reserved
publishers may finish publication after cancellation, with their body subject
to the normal cancellation check. Normal external/child spawn and successful
completion without combine dependencies take no runtime mutex. Mutexes remain
on the error and dependent-registration paths; this is not a guarantee that
the allocator or the standard library's atomic-wait implementation is lock-free.

## Exceptions and cleanup

The first callback or construction/publication error is retained, and closes
both admission paths. Concurrent failures select the first recorded under the
separate error mutex; all result observers and `join()` see that same error.
Publication failure also rethrows to the caller of `spawn`, even if the caller
catches it; catching does not clear the scope error. Previously accepted tasks
continue draining. `last_error()` observes the retained exception.

Each accepted submission holds a publisher credit while constructing the callable
and submitting its packet. A separate fork is transferred to the executor. The
publisher credit protects rollback even if packet allocation fails and the
`enqueue` parameters are destroyed in either order. The executor destroys the
callable and its captures before retiring its credit. Target destructors must
be nonthrowing. Callable storage follows `small_function`'s inline/spill policy.

The caller's own callable object and argument temporaries remain caller-owned;
their destruction is not part of scope completion. References and pointers in
captures remain the caller's responsibility.

## Ownership boundary

The scope owns a separately allocated state. Tasks borrow that state under their
credits; destruction waits before releasing it. A task receives a borrowed,
noncopyable, nonmovable `Context&` referring to its executor credit. Do not store
it beyond callback return or pass it to independently running work. Use it for
child submission rather than capturing the scope owner, which may already be
executing its destructor. `Pool::submit` or detached work launched manually from
a callback is outside the group; use `ctx.spawn` to include descendants.

After final completion, task publication, invocation and user capture cleanup
have all stopped dereferencing scope state. The owner may still inspect its
result until destruction. Pool epilogues and completion dependency propagation
can continue using their own state. A copied `Handle` may outlive both the scope
and pool after completion and preserves the result without granting admission.

The pool must outlive every scope using it. External `spawn`, `close`, `cancel`,
`wait`, `join` and observations may race while the scope object remains alive;
external calls must finish before its destruction. Callback access uses Context.
Worker waits help execute queued tasks, including in a one-worker pool.

Waiting on one's own scope, or an active ancestor on the same thread, throws
`std::logic_error`. Detection covers callback execution, callable construction
and destruction, and nested cooperative waiting. Destroying a scope from its
own active work calls `std::terminate`, since it cannot join itself. Avoid cyclic
waits across independently running scopes/threads; this is not a general deadlock
detector. Calling `Pool::wait(scope.completion())` directly bypasses the scope's
self-join check and must not be done from work contributing to that completion.

`tests/task_scope_tests.cpp` exercises admission races, late descendants, draining,
cancellation, concurrent errors, self/ancestor joins, capture cleanup and observer
survival. The system-backend failure tests inject callable and packet allocation
failures on both submitting and worker threads.
