# Runtime protocol mutation campaign

`dagflow_protocol_mutation_campaign` compiles disposable variants of actual
runtime translation units and headers. It tests ordering, packet custody,
liveness credits, cancellation and error/dependency propagation. The working
tree and ordinary libraries are never patched. This opt-in target uses the
system allocator, GCC/Clang, C++23 and `DAGFLOW_FUZZ_HOOKS` on UNIX.

```sh
cmake --preset core
cmake --build --preset core --target dagflow_protocol_mutation_campaign
python3 scripts/protocol_mutation_campaign.py --cxx clang++ \
  --report out/protocol-mutations/report.json
python3 scripts/protocol_mutation_campaign.py --list
python3 scripts/protocol_mutation_campaign.py \
  --mutant pool-ready-before-packet-destroy
```

Every selected oracle must pass its baseline first. A source mutant replaces
one baseline object. Header mutants recompile **all** runtime units and their
selected test against one overlay, avoiding mixed inline definitions and ODR
violations. Anchors must match exactly once; stale anchors are build errors.
Variants live in a temporary directory. JSON persists after cleanup with
baseline output, per-mutant status, duration, invariants and replay traces.

## Counterfactuals and observed results

| Mutation | Oracle / protocol | Result |
| --- | --- | --- |
| Drop final graph token | Independent DAG/token reference model | Killed |
| Wrong last-predecessor arrival predicate | Dependency fan-in model | Killed |
| Execute graph token twice | Exactly-once graph model | Killed |
| Skip payload destruction | Completion lifetime PBT | Killed |
| Suppress dependent error propagation | Source-set/error reference model | Killed |
| Suppress first-error publication | First-error reference model | Killed |
| Ignore graph cancellation | Dependency/continuation schedules | Killed |
| Lose stolen batch sibling | Scheduler custody schedules | Killed |
| Return already-republished sibling | Unique-custody schedules | Killed |
| Publish ready before payload cleanup | Credit retirement/registration schedules | Killed |
| Omit fork credit increment | Independent live-credit conservation oracle | Killed |
| Finish packet credit before callable destruction | Pool/Scope cleanup-before-ready schedules | Killed |
| Ignore TaskScope cancellation observation | Cancel/execution schedules | Killed |
| Omit parking epoch increment | Announce/final-scan/park/wake schedules | Killed |
| Omit `notify_one` | Parking schedules with real CV tail | **Survived** |
| Weaken deque release publication to relaxed | SC queue schedules | **Survived** |

Result: **14/16 assertion-detected, 2 survived, 0 process timeouts, 0 crashes,
0 build errors**. These chosen mutants are a diagnostic set, not an estimate
of overall correctness or a global mutation score.

Only the PBT runner's `property failed:` report or a schedule assertion with
exit 86 and `schedule property failed:` counts as a kill. PBT prints minimized
operation traces; schedule failures print a concrete actor prefix. Watchdog
exit 87, process timeout, crash without an assertion report, build error and
survivor are separate results. The command exits zero only if every selected
mutant is assertion-detected. The full campaign therefore currently exits
**1**, because of the two survivors; it is not an ordinary green CTest gate.

The missing-notify survivor identifies a verification limit: a predicate wait
can observe the changed epoch when its deadline expires and return success.
These tests do not assert a host-time latency threshold. The relaxed-store
survivor identifies another limit: controller synchronization is stronger than
unconstrained runtime execution. Weak memory needs a memory-model-aware checker
or additional appropriate hardware experiments.

See [`schedule-exploration.md`](schedule-exploration.md) for exact actor,
packet and checkpoint bounds, replay commands and the unenumerated CV tail.
