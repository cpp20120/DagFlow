# Adversarial runtime regressions

See the [test suite guide](README.md) for ordinary runtime tests, allocation
checks, and build integration coverage.

These thirteen executables are normal CTest tests, enabled by
`DAGFLOW_BUILD_TESTS=ON` when a runtime library is built. They require neither
`DAGFLOW_FUZZ_HOOKS` nor a fuzzing build. Each has a 60-second CTest timeout;
lost-wakeup checks use a longer parking timeout so polling cannot rescue them
before CTest reports a hang.

| Test suffix (target prefix: `dagflow_`) | Invariants exercised |
| --- | --- |
| `pool_address_reuse_tls_test` | Four persistent producer threads, eight fixed pool addresses, sixteen reconstruction generations, changing sparse topology; descendants and capture destruction drain before pool destruction; old observers/errors survive all generations. |
| `completion_last_credit_race_test` | Eight retiring credits publish plain writes to terminal payload destruction; blocked destruction keeps source and dependent handles unready; late registration, observer churn, duplicate edges and waiters race with retirement. |
| `parking_epoch_reuse_test` | Cancel/rearm and remote wake across packed membership offsets 63/64 and 127/128; six owners repeatedly park, with and without a competing `wake_all`; epochs never regress. |
| `reentrant_capture_cleanup_test` | A throwing task's capture destructor submits and waits for another throwing task after close; nested cleanup publishes a grandchild; range callable destruction reenters the only worker before range readiness. |
| `multi_source_completion_failure_race_test` | Eight independent sources retire while two threads register duplicate fan-in/diamond dependencies; only reachable failures propagate, the first error identity stays stable, and observers survive pool destruction. |
| `completion_reentrant_registration_test` | Terminal payload destruction registers dependents on its own unfinished completion, retires another source recursively, and drops its own observer, including after pool shutdown. |
| `graph_scope_unwind_cleanup_test` | An unstarted graph runs during exception unwinding on the only worker; inner failure preserves the outer exception; throwing callable copies at each chunk boundary leave no leaked captures or partial graph node. |
| `combine_partial_registration_oom_race_test` | Fail each dependent-registration allocation after a different accepted prefix, release two retiring threads at that failure, and check that every abandoned completion/edge allocation is reclaimed. |
| `range_publication_rollback_lifetime_test` | Fail publication after the first range packet is accepted, keep that packet blocked on borrowed local storage, and require drain/capture cleanup before the exception escapes; covers both range algorithms. |
| `cancelled_capture_reentry_test` | Cancel queued children on the only worker; skipped captures reject publication and self-join while recursively helping independent tasks; a fresh scope remains healthy. |
| `nested_helping_context_restore_test` | Eight nested scopes help on the only worker, with alternating failures and closed admission; ancestor self-join detection and descendant authority survive unwinding, and subsequent tasks inherit no stale frame. |
| `graph_cancel_continuation_reuse_race_test` | A driver, throwing branch and external canceller meet near invocation boundaries 64/128; immediately reuse the graph after readiness without waiting for pool idle, while retaining immutable old errors. |
| `exception_payload_last_release_test` | Destroy the source pool, concurrently release fan-in observers on eight foreign threads, then destroy the last exception-owned resource on a designated thread; its destructor releases another completion and submits/waits in a surviving pool. |

Build the complete group in a configured build directory:

```sh
cmake --build out/build/adversarial-check --parallel 4 --target dagflow_adversarial_tests
ctest --test-dir out/build/adversarial-check -L adversarial --output-on-failure
```

Use the same label in builds configured with
`BOILERPLATE_SANITIZER=address-undefined` or `BOILERPLATE_SANITIZER=thread`.
The two allocation-failure tests link a private copy of the runtime with a
test allocation backend. It replaces `allocate_bytes`/`deallocate_bytes`, counts
live runtime allocations, and injects one thread-local failure at an explicit
barrier. It does not replace global `new`/`delete`, so these tests also run under
TSan. They test runtime rollback rather than a particular allocator backend.

For a dedicated build with the minimum supported two-slot local and central
queues and a default central batch of one:

```sh
cmake --preset adversarial-tiny
cmake --build --preset adversarial-tiny
ctest --preset adversarial-tiny
```

This preset builds/runs only the adversarial group. Its
`DAGFLOW_TEST_TINY_QUEUES` setting is applied to every translation unit in that
build, including the private allocation-failure runtime. Installation is
disabled because queue capacities change template layouts. Ordinary builds
keep their existing capacities and batch defaults.

Barriers and latches establish the required lifetime boundaries without sleeps.
Thread interleavings still depend on scheduling; these regressions do not prove
the absence of every possible race and do not replace fuzz campaigns.
