# Public API examples

Build all examples and their smoke checks:

```sh
cmake -S . -B out/build/examples -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DDAGFLOW_BUILD_EXAMPLES=ON -DDAGFLOW_BUILD_TESTS=ON
cmake --build out/build/examples -j 4
ctest --test-dir out/build/examples -R 'dagflow_(example_|api_convenience_tests)' \
  --output-on-failure
```

The default allocator is mimalloc; add `-DDAGFLOW_ALLOCATOR=system` if it is not
installed. Executable names below assume an empty `BOILERPLATE_ARTIFACT_SUFFIX`.
These are short examples with checked results; the separate stress harness lives in `bench/stress_harness.cpp`.

| Source | Executable | Public API demonstrated |
| --- | --- | --- |
| [Quick start](basic.cpp) | `dagflow-example` | `Pool::submit`, move-only capture, `combine`, `wait_and_rethrow`, task errors |
| [Task scope](task_scope.cpp) | `dagflow-example-task_scope` | `TaskScope::spawn`, `Context::spawn`, `close`, `join`, errors |
| [Cancellation](cancellation.cpp) | `dagflow-example-cancellation` | Cooperative cancellation of a running callback, rejected submission |
| [Graphs](graph.cpp) | `dagflow-example-graph` | `GraphScope::emplace/then/when_all`, explicit `TaskGraph` edges/seal, repeated runs |
| [Ranges](parallel_for.cpp) | `dagflow-example-parallel_for` | Container/span overloads of `for_each`, `for_each_ws`, `parallel_for`, `parallel_for_after` |
| [Batch submission](batch.cpp) | `dagflow-example-batch` | `submit_batch_detached`, moved callables, partial-publication cleanup, `wait_idle` |

## Choosing an API

- Submit a single asynchronous callback with `Pool::submit`; `Handle` observes
  completion and errors, not a returned value. `wait_and_rethrow` waits and then
  propagates failure. `wait` remains completion-only. Workers help execute queued
  work while waiting with either method.
- Use `TaskScope` for a dynamic family of tasks borrowing local data. Declare the
  data before the scope so scope destruction joins before the data is destroyed.
  `join` closes admission, waits for all descendants, and propagates errors;
  destruction waits but does not throw. Children use their callback's `Context`.
- Use `GraphScope` for convenient reusable dependencies, or `TaskGraph` for
  explicit edges, compilation and cancellation. `emplace` builds a node without
  running it. `GraphScope` destruction also executes unstarted pending work;
  `TaskGraph` destruction waits for active work but does not start an unrun graph.
- Use detached submission when you deliberately do not need per-task completion
  or propagated callback errors. Keep borrowed data alive through an explicit
  barrier. Destroying a `Pool` is not a substitute for `wait_idle()`.

## Range ownership and scheduling

```cpp
std::vector<int> data(1000);
auto result = pool.for_each(data, [](int& x) { ++x; });
pool.wait_and_rethrow(result);
```

The range overloads accept lvalue ranges and temporary borrowed ranges such as
`std::span(data)`. An owning temporary such as `std::vector<int>(1000)` is rejected.
No data is copied or retained by ownership: backing storage, and any view object
needed by its iterators, must remain alive without iterator invalidation until
completion. `std::move(data)` is rejected as well. Passing a span does not extend
the storage's lifetime.

`for_each` and the graph range algorithms require a forward range;
`for_each_ws` requires random access. Non-common ranges with a different sentinel
type are supported; forming their iterator end may require a traversal. Ranges
must be finite. The existing iterator overloads remain available.

Pool algorithms share one callable instance between concurrent tasks. Use
thread-safe captures; distinct elements alone do not make shared callback state
safe. Graph algorithms copy the callable per chunk and retain the iterators for
future runs, so keep their range valid until graph clearing/destruction.
`NodeOptions::concurrency` limits active executions, and `capacity` controls token
admission; neither specifies a chunk size. `for_each_ws` has an explicit grain hint.
