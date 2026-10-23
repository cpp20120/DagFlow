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
| Omit `notify_one` | Untimed parking with notification obligation | Killed |
| Propagate after any lane instead of the final lane | Four-token/two-lane graph schedules | Killed |
| Bypass the predecessor fan-in counter | Controlled two-predecessor fan-in schedules | Killed |
| Stop workers before physical drain | Nested helping/shutdown schedules | Killed |
| Weaken deque release publication to relaxed | SC queue schedules | **Survived here; detected by the separate weak-memory check** |

Result: **18/19 assertion-detected, 1 survived, 0 timeouts, 0 crashes,
0 build errors**. These chosen mutants are a diagnostic set, not an estimate
of overall correctness or a global mutation score.

Only the PBT runner's `property failed:` report or a schedule assertion with
exit 86 and `schedule property failed:` counts as a kill. PBT prints minimized
operation traces; schedule failures print a concrete actor prefix. Watchdog
exit 87, process timeout, crash without an assertion report, build error and
survivor are separate results. The command exits zero only if every selected
mutant is assertion-detected. The full campaign therefore currently exits
**1**, because of the weak-memory survivor; it is not an ordinary green CTest gate.

The missing-notify mutation now fails the explicit obligation
`committed sleeper requires an actual CV notification`, with trace
`0,0,0,0,1,1,1` in `untimed-notification`. The hook-only CV wrapper disables the
timer fallback and prevents spurious wakes from discharging this obligation.
It counts inside `notify_one()` itself, so deleting the call deletes the event.
This is an assertion kill, not a watchdog or a host-time latency threshold.
The relaxed-store survivor identifies a remaining limit: controller
synchronization is stronger than unconstrained runtime execution. Weak memory
needs a memory-model-aware checker or additional appropriate hardware experiments.

The separate [Relacy campaign](weak-memory.md) now checks this exact publication
mutation against a copy of the current deque with a simulated atomic backend.
Its publication cases require a counterexample, so that campaign passes only
when the baseline is clean and the relaxed mutation is detected. This does not
change the SC campaign's observed 18/19 result or its nonzero exit status.

See [`schedule-exploration.md`](schedule-exploration.md) for exact actor,
packet and checkpoint bounds, replay commands and the unenumerated CV tail.
