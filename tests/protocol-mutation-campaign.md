# TaskGraph / Completion mutation campaign

The `dagflow_protocol_mutation_campaign` target compiles **disposable** variants of
`src/task_graph.cpp` and `src/thread_pool.cpp`, retaining all other runtime
translation units unchanged. Neither the working tree nor ordinary DagFlow
libraries are mutated. This is a separate opt-in target, not part of CTest or
coverage builds. Currently supported by the direct compiler runner on UNIX
with GCC/Clang and the built-in system allocator.

```sh
cmake --preset core
cmake --build --preset core --target dagflow_model_property_tests -j 4
ctest --preset core -L pbt --output-on-failure
cmake --build --preset core --target dagflow_protocol_mutation_campaign
```

Or adjust the campaign exploration budget directly:

```sh
python3 scripts/protocol_mutation_campaign.py --seed 496833221358 --cases 48 --steps 48
```

Each variant compiles against the actual runtime and runs one of the existing
reference PBT executables. The **baseline must pass** before any mutant is
judged. Only a `property failed:` report from the PBT runner is classified as
`KILLED(property)`; a normal exit is `PASS` (survived), whereas a crash,
compiler failure or timeout is **not** counted as a successful assertion kill.

| Component | Mutation | Observable contract |
|---|---|---|
| TaskGraph | Drop the final token | Exact token execution count |
| TaskGraph | Change last-predecessor arrival predicate | Predecessor ordering / fan-in |
| TaskGraph | Invoke a token twice | Exactly-once token execution |
| Completion | Skip payload destruction | Destruction at final credit retirement |
| Completion | Do not propagate dependent error | Fan-in preserves source failures |
| Completion | Suppress first-error publication | First-error-wins for shared credits |

The additional `dagflow_completion_failure_reference_property_tests` stateful
property exercises registered and late joins, extra credits, different credit
retirement orders, a concurrent last-credit/registration race, errors originating from either credit, and an unrelated
source keeping the fan-in incomplete. It was added because both failure mutants
**survived** the initial campaign even though the preexisting models were green.

A perfect score on these six handpicked mutants is **not** a global mutation
score, a proof of linearizability, or a proof of memory safety. Mutants that
change time-sensitive shutdown/cancellation races require independent bounded
concurrency histories and more precise scheduler hooks. The compiler-driven
campaign intentionally uses a fixed `-O0` system allocator build to avoid
mixing production optimization or allocator policies into the results.
