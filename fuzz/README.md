# DagFlow fuzzing by layer

The runtime and harnesses use the dagflow fuzz API. `dagflow_fuzz_runtime`
is a private instrumented archive compiled from the same source list as the
production library. Normal shared/static targets, examples and package exports
remain uninstrumented unless their own build policy requests sanitizers.
No separate compiler or sanitizer flags are maintained in the DagFlow adapter.

Scheduling checkpoints are currently commented out in the runtime sources.
There are **18 active targets**: `wake_protocol` is excluded from CMake and CI,
and `saturation` input mode 2 (external backpressure) is a no-op. Both scenarios
wait for checkpoint events and must remain disabled until those calls return.
Their source and seeds are retained. Allocation-failure injection remains active;
the other harnesses still run their checks, but `Perturb` receives no events.

| Target | Layer and oracle |
| --- | --- |
| `dagflow_fuzz_containers` | `small_vector`: inline/heap transitions, reserve, moves, aliasing insertion and bounds; compares values to `std::vector` and checks live-object counts |
| `dagflow_fuzz_function` | `small_function` and `inplace_function`: inline, oversized and over-aligned captures, move/reset/invocation; checks values, alignment and exact destruction |
| `dagflow_fuzz_queues` | MPMC ring and Chase–Lev deque: FIFO/LIFO reference models, capacity/wraparound, concurrent publishers/consumers and owner/thief races; compares accepted and consumed IDs |
| `dagflow_fuzz_scope` | TaskScope: children, external close/cancel, exceptions, explicit/destructor join; checks completion, admission, capture destruction and at-most/exactly-once execution |
| `dagflow_fuzz_pool` | Submission, batches, ranges, cooperative waits, external producers, idle/close/shutdown; checks task accounting and output buffers |
| `dagflow_fuzz_graph` | DAG topology, cycle rejection, tokens, predecessor ordering, concurrency limits, overflow policies, cancellation during a live callback, exception propagation and run reuse |
| `dagflow_fuzz_parking` | Epoch registration, cancel/wake races, CV handshakes and idle bitmap boundaries (63/64/65 and 127/128/129 workers, without creating that many OS threads) |
| `dagflow_fuzz_lifecycle` | Concurrent admission, close/shutdown, child publication and idempotent shutdown; checks accepted task/descendant execution |
| `dagflow_fuzz_accounting` | Producer churn, retired/published snapshots, no premature `wait()` return and repeated accounting identity |
| `dagflow_fuzz_scheduler` | Local/central/overflow custody paths, batch stealing and exactly-once packet retrieval |
| `dagflow_fuzz_credits` | CompletionCredit fork/move/finish, sentinel, payload destruction before readiness and error propagation |
| `dagflow_fuzz_allocation` | Fault injection at the runtime allocator boundary, scalar/batched submission and seal recovery; respects accepted-prefix semantics |
| `dagflow_fuzz_exceptions` | Throw at each copy/move boundary during vector spill, reserve and inline moves; checks strong/basic guarantees, aligned storage, live objects and recovery. Callable construction/allocation failures must preserve the previous target |
| `dagflow_fuzz_ranges` | Empty and 16K/32K chunk boundaries, forward iterators, non-common sentinels, spans and iota; work-stealing grain extremes, callback/publication failures, exactly-once visits and callable destruction before completion/error return |
| `dagflow_fuzz_graph_scope` | Implicit/destructor execution, duplicate run/wait, active/pending clear, rebuild, failure/reuse, duplicate fan-in edges and multi-chunk dependent ranges with guard elements |
| `dagflow_fuzz_joins` | Completion DAGs with duplicate/invalid/ready sources, registration versus retirement, partial allocation failure, first-error propagation and up to 2048 pending dependent levels; observer lifetime beyond pool destruction |
| `dagflow_fuzz_saturation` | Real 1024/16384-slot queues, automatic local spill and worker overflow, repeated wraparound and bounded same-priority ingress/overflow service under continuous local load; external backpressure mode is currently disabled |
| `dagflow_fuzz_scope_races` | Concurrent external spawn/close/cancel/wait/join, descendants after close, reserved callable construction during cancellation, producer/worker allocation failures, self/ancestor joins and destructor reentrancy |
| `dagflow_fuzz_wake_protocol` (disabled) | Preserved full-pool idle registration/final scan/CV handshakes, one-wake batch recruitment, empty/sparse shard domains, shutdown notification and completion readiness before physical accounting retirement |

All inputs are bounded byte streams. Harnesses obey lifetime and concurrency
preconditions; malformed input never intentionally creates dangling references,
multiple Chase–Lev owners or invocation of empty callables. The low-level layers
run independently of the scheduler; scope/pool/graph cover runtime integration.

```sh
cmake --preset fuzz
cmake --build --preset fuzz --parallel 4
```

The commands above only configure and build. They do not execute harnesses.
Run replay or campaigns separately when wanted:

```sh
ctest --preset fuzz --no-tests=error

# Build + seed replay through the toolkit aggregate:
cmake --build out/build/fuzz --target fuzz-smoke

# Explicit coverage-guided campaign; default duration is 60 seconds:
cmake --build out/build/fuzz --target fuzz_dagflow_fuzz_graph
```

`DAGFLOW_FUZZ_LAYERS` selects a semicolon-separated subset of the active table entries. The default builds eighteen layers.
An existing build caches the old list; use `cmake --preset fuzz -U DAGFLOW_FUZZ_LAYERS`
to adopt the new default, or explicitly select the desired layers. This does not
remove or replay the accumulated build-tree corpus.
Selecting the disabled `wake_protocol` layer produces a configuration error.
Old binaries/corpus directories can remain in an existing build tree; use the
configured targets and CTest registrations rather than discovering targets by
globbing those leftover directories.
`DAGFLOW_FUZZ_RUNTIME` sets the campaign duration and
`DAGFLOW_FUZZ_TIMEOUT` the libFuzzer per-input timeout (default 15 seconds).
`DAGFLOW_FUZZ_SANITIZER` accepts `none`, `address`, `undefined`, or
`address-undefined` (default). UB is fatal so sanitizer findings stop the run.

Each target copies `fuzz/corpus/<layer>/` into its own build-tree working corpus,
`<build>/dagflow_fuzz_<layer>-corpus/`. Mutations and crash artifacts never write
into the checked-in seeds. Reproduce a finding by passing the artifact directly:

```sh
out/build/fuzz/dagflow_fuzz_graph /path/to/crash-input -runs=1
```

`DAGFLOW_FUZZ_BACKEND` selects `libfuzzer` (the supplied preset), `aflpp` or
`honggfuzz`. For the latter two, select the corresponding compiler wrapper in a
fresh build before `project()`. The same backend and sanitizers apply to both
the instrumented runtime and harnesses. The newest layers have only been built
with native Clang/libFuzzer; their runtime replay is still pending. The generic
wrapper checks remain in dagflow.

CTest replays the corpus; it is not a long mutation campaign. The CI job also
budgets 200 additional executions beyond seed initialization per layer and
retains crash artifacts on failure. Thread
schedules are not coverage-guided, so repeat corpora to sample interleavings.
The `queues` target also checks bounded concurrent histories against a FIFO
linearizability oracle, not only accepted/consumed ID sets.

### Adversarial inputs

The four additional layers (`exceptions`, `ranges`, `graph_scope`, `joins`) have
714 deterministic seeds generated by `python3 fuzz/generate_adversarial_corpus.py`.
The same generator also writes 737 seeds for `saturation` (224), `scope_races`
(129) and `wake_protocol` (384), for 1451 adversarial seeds in total.
This count includes the retained seeds for the currently disabled scenarios.
The generator only writes seed files; it never builds or executes a harness.
Seeds enumerate vector construction cutoffs, range/chunk boundaries, scope
lifecycle modes and join registration failures so these cases do not depend on
mutations discovering a long valid prefix first. Existing layers and their input
layouts are unchanged, preserving the usefulness of accumulated corpora.

Range inputs include 0, 1, 2, 31/32/33 and 16383/16384/16385,
32767/32768/32769 elements. Work-stealing grains include zero and `SIZE_MAX`.
`exceptions` distinguishes a copy relocation's strong guarantee from a throwing
move's basic guarantee; moved-from elements are checked against the injected
failure position, then cleared and reused. `joins` holds its first source open
until the deep chain is registered, ensuring terminal propagation actually has
pending dependents to retire. Its error oracle accepts any failing ancestor
when failures race, while checking first-error retention on each source.

`saturation` uses production queue capacities, not a smaller test substitute.
Single-worker parents publish more than 17K tasks before helping, forcing the
real overflow routing. In the currently disabled external backpressure mode,
external publishers report a failed ingress push through
the fuzz-only `external_retry` checkpoint; only then does the harness close
admission and release the blocked worker. A separate scheduler oracle keeps
local, ingress and overflow sources busy across shard rotations and checks
bounded service for equal-priority work. It does not require Normal work to
preempt an endless stream of High-priority work.

`scope_races` varies both concurrency and lifecycle operations. Its publication
gate pauses inside a reserved callable copy, proving completion stays pending
until the publisher leaves even if close/cancel wins. Allocation failures are
scoped to external or child publication, then disabled before worker cleanup.
Ancestor-join checks force a single worker so cooperative helping retains the
active ancestor stack; they do not create unsupported cross-thread wait cycles.

The preserved, currently disabled `wake_protocol` gates all actual workers at `idle_announce` or `before_cv`,
publishes a single batch, then requires every worker to enter a blocking
callback. At `before_cv` it also checks one wake claim per worker. The parking
timeout is deliberately longer than the normal libFuzzer input timeout, so
periodic wakeups cannot conceal a broken recruitment chain. Shutdown and a
`before_retire` gate exercise stop notification and ready-versus-idle ordering.
These deterministic gates rely on the runner's per-input timeout for deadlock
detection; unlike `Perturb`, they do not expire and skip a missing peer.
Replay of the active `saturation` and `scope_races` corpora has a 120-second CTest allowance;
the libFuzzer per-input timeout remains 15 seconds by default.

These are additional fault-seeking scenarios, not a measured coverage gain.
Compiling the targets alone does not validate their runtime assertions; replay
and sanitizer campaigns are still separate, explicit actions.

`DAGFLOW_FUZZ_HOOKS` is defined only for the private instrumented runtime and
fuzz targets. `fuzz/concurrency.hpp` installs an optional callback at selected
*unlocked* protocol checkpoints (`idle_announce`, `before_cv`, `wake_claim`,
`external_publish`). The byte stream selects yields and optionally arms one
checkpoint until a second is reached; waits are capped at 200 microseconds.
A missing release event times out rather than freezing the campaign. This is
**bounded scheduling perturbation**, not deterministic replay or exhaustive
model checking. Do not interpret `-runs=N` as `N` distinct interleavings.
With the runtime calls commented out, this callback is not reached; the
description above documents how scheduling perturbation works when enabled.
The fuzz-only allocation budget is thread-local and only targets calls into
`dagflow::detail::allocate_bytes()`; it is not a global `operator new` failure
injector. The other, non-instrumented production targets do not inherit hooks.
ASan/UBSan do not replace TSan; use the separate `check-tsan` runtime tests for
race detection. In restricted/ptraced environments where LeakSanitizer cannot
start, `ASAN_OPTIONS=detect_leaks=0` allows ASan/UBSan checks; it disables leak
checking only for that invocation, not in the preset or project defaults.

### Explicit limitations

* Checkpoint perturbations use a bounded two-point gate plus occasional
  `yield()` calls. They can delay a thread briefly but do not record/replay
  deterministic schedules or enumerate all interleavings. The `parking` and
  `lifecycle` tests cover selected orderings, not all of them.
* `parking` checks the direct ParkingLot contract. Full-pool final-scan/recruit
  orderings in `wake_protocol` are currently disabled along with the checkpoints.
  Neither harness enumerates every schedule or proves the memory model correct.
* `allocation` injects failure only in the caller thread's runtime allocator
  boundary; it deliberately does not sabotage worker epilogues.
* `accounting` tests a stable set of already-published credits while they
  retire. New external submissions after idle observation are legal.
* The default fuzz preset still selects the system allocator. Alternative
  allocator backends, OS thread-creation failures and every configuration/error
  branch are not covered by these additions.
* This setup instruments only Clang/libFuzzer + ASan/UBSan; test with TSan
  separately and never compare instrumented performance against Release.
