#!/usr/bin/env python3
"""Mutation test TaskGraph and Completion against their actual C++ property models.

All source variants and objects are created under a disposable temp directory.
The checked-out runtime is never patched. Only an assertion-based property
failure counts as an identified mutant; crashes, timeouts and compile errors
remain distinct outcomes, not successful kills.
"""
from __future__ import annotations

import argparse
import json
import time
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ["thread_pool", "scheduler", "parking_lot", "idle_accounting",
           "task_graph", "task_scope", "runtime_memory", "runtime_diagnostics"]
MODELS = {
    "graph": "reference_graph_model_property_tests.cpp",
    "completion": "completion_property_tests.cpp",
    "failure": "completion_failure_reference_property_tests.cpp",
    "schedule_scheduler": "scheduler_schedule_exploration_tests.cpp",
    "schedule_completion": "completion_schedule_exploration_tests.cpp",
    "schedule_parking": "parking_schedule_exploration_tests.cpp",
}


def replace_once(source: str, before: str, after: str) -> str:
    found = source.count(before)
    if found != 1:
        raise ValueError(f"Expected unique mutation anchor, found {found}: {before[:90]!r}")
    return source.replace(before, after, 1)


# Mutations are deliberately small violations of distinct runtime contracts.
# Each is assigned an appropriate existing *independent* stateful oracle.
MUTANTS = {
    "graph-drop-last-token": (
        "task_graph", "graph",
        "if (token >= node.tokens) break;",
        "if (token + 1 >= node.tokens) break;"),
    "graph-ignore-predecessor-join": (
        "task_graph", "graph",
        "state.runtime[j].predecessors.fetch_sub(\n                1, std::memory_order_acq_rel) != 1",
        "state.runtime[j].predecessors.fetch_sub(\n                1, std::memory_order_acq_rel) != 2"),
    "graph-duplicate-token": (
        "task_graph", "graph",
        "node.work(token);",
        "node.work(token); node.work(token);"),
    "completion-drop-payload-destruction": (
        "thread_pool", "completion",
        "      destroy(payload);",
        "      (void)destroy; (void)payload;"),
    "completion-drop-dependent-error": (
        "thread_pool", "failure",
        "      if (failure) dependent->set_error(failure);",
        "      if (false) dependent->set_error(failure);"),
    "completion-ignore-first-error": (
        "thread_pool", "failure",
        "  if (!error) {\n    error = std::move(failure);",
        "  if (false) {\n    error = std::move(failure);"),
}


MUTANTS.update({
    "graph-ignore-cancel": (
        "task_graph", "schedule_completion",
        "      if (!state.cancel.load(std::memory_order_acquire)) {",
        "      if (true) {"),
    "scheduler-drop-stolen-sibling": (
        "scheduler", "schedule_scheduler",
        "if (!destination.try_push(tasks[i])) std::terminate();",
        "(void)tasks[i];"),
    "scheduler-duplicate-stolen-sibling": (
        "scheduler", "schedule_scheduler",
        "return tasks[0];", "return taken > 1 ? tasks[1] : tasks[0];"),
    "completion-ready-before-cleanup": (
        "thread_pool", "schedule_completion",
        "    if (current->destroy_payload) {",
        "    current->ready.store(true, std::memory_order_release);\n"
        "    if (current->destroy_payload) {"),
    "completion-drop-fork-credit": (
        "thread_pool", "schedule_completion",
        "  state_->credits.fetch_add(1, std::memory_order_relaxed);",
        "  (void)state_->credits;"),
    "pool-ready-before-packet-destroy": (
        "thread_pool", "schedule_completion",
        "  ops->destroy(task);\n  DAGFLOW_FUZZ_POINT(task_after_destroy);\n  done.finish();",
        "  done.finish();\n  ops->destroy(task);\n  DAGFLOW_FUZZ_POINT(task_after_destroy);"),
    "scope-ignore-cancellation": (
        "include/dagflow/task_scope.hpp", "schedule_completion",
        "    if (cancelled) return;", "    if (false && cancelled) return;"),
    "parking-drop-epoch": (
        "parking_lot", "schedule_parking",
        "  waiter.epoch.fetch_add(1, std::memory_order_seq_cst);",
        "  (void)waiter.epoch;"),
    "parking-drop-notify": (
        "parking_lot", "schedule_parking",
        "  waiter.cv.notify_one();", "  (void)waiter.cv;"),
    # An intentional weak-memory probe: a synchronized SC explorer on x86 is
    # not expected to reliably distinguish this from release publication.
    "deque-relax-publication": (
        "include/dagflow/detail/chase_lev_deque.hpp", "schedule_scheduler",
        "    DAGFLOW_FUZZ_POINT(deque_before_publish);\n"
        "    bottom_.store(b + 1, std::memory_order_release);",
        "    DAGFLOW_FUZZ_POINT(deque_before_publish);\n"
        "    bottom_.store(b + 1, std::memory_order_relaxed);"),
})


def run(cmd: list[str], *, timeout: int, env: dict[str, str] | None = None):
    return subprocess.run(cmd, env=env, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout)


def build(cxx: str, args: list[str], source: Path, output: Path) -> None:
    process = run([cxx, *args, "-c", str(source), "-o", str(output)], timeout=120)
    if process.returncode:
        raise RuntimeError(f"compiling {source.name}:\n{process.stdout[-3500:]}")


def execute(binary: Path, seed: str, cases: int, steps: int, timeout: int):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("DAGFLOW_PROPERTY_", "DAGFLOW_SCHEDULE_"))}
    env.update(DAGFLOW_PROPERTY_SEED=seed, DAGFLOW_PROPERTY_CASES=str(cases),
               DAGFLOW_PROPERTY_STEPS=str(steps))
    try:
        proc = run([str(binary)], env=env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        return "TIMEOUT", output.decode(errors="replace") if isinstance(output, bytes) else output
    if proc.returncode == 0:
        return "SURVIVED", proc.stdout
    if proc.returncode == 87 and "schedule timeout:" in proc.stdout:
        return "TIMEOUT", proc.stdout
    if proc.returncode == 86 and "schedule property failed:" in proc.stdout:
        return "KILLED(schedule)", proc.stdout
    if "property failed:" in proc.stdout:
        return "KILLED(property)", proc.stdout
    return f"CRASH(exit={proc.returncode})", proc.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cxx", default=os.environ.get("CXX") or shutil.which("c++"))
    parser.add_argument("--seed", default="496833221358")
    parser.add_argument("--cases", type=int, default=48)
    parser.add_argument("--steps", type=int, default=48)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--mutant", action="append", choices=sorted(MUTANTS))
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--report", type=Path,
                        default=ROOT / "out/protocol-mutations/report.json")
    parsed = parser.parse_args()
    if parsed.list:
        print("\n".join(MUTANTS))
        return 0
    if not parsed.cxx or min(parsed.cases, parsed.steps, parsed.timeout) < 1:
        parser.error("compiler and positive exploration budgets required")
    selected = parsed.mutant or list(MUTANTS)
    flags = ["-std=c++23", "-O0", "-g0", "-pthread", "-DDAGFLOW_STATIC",
             "-DDAGFLOW_FUZZ_HOOKS", "-I", str(ROOT / "include"),
             "-I", str(ROOT / "tests")]
    report = {"compiler": parsed.cxx, "seed": parsed.seed,
              "cases": parsed.cases, "steps": parsed.steps,
              "baseline": {}, "mutants": {}}

    def save():
        parsed.report.parent.mkdir(parents=True, exist_ok=True)
        parsed.report.write_text(json.dumps(report, indent=2) + "\n")

    def link(objects, test, binary):
        proc = run([parsed.cxx, "-pthread", *map(str, objects.values()),
                    str(test), "-o", str(binary)], timeout=80)
        if proc.returncode:
            raise RuntimeError(f"linking {binary.name}: {proc.stdout[-3000:]}")

    try:
        with tempfile.TemporaryDirectory(prefix="dagflow-protocol-mutants-") as temp:
            directory = Path(temp)
            objects = {}
            for name in RUNTIME:
                objects[name] = directory / f"{name}.o"
                build(parsed.cxx, flags, ROOT / "src" / f"{name}.cpp", objects[name])
            tests = {}
            for model in dict.fromkeys(MUTANTS[name][1] for name in selected):
                tests[model] = directory / f"{model}.o"
                build(parsed.cxx, flags, ROOT / "tests" / MODELS[model], tests[model])
                binary = directory / f"baseline_{model}"
                link(objects, tests[model], binary)
                status, output = execute(binary, parsed.seed, parsed.cases,
                                         parsed.steps, parsed.timeout)
                passed = status == "SURVIVED"
                report["baseline"][model] = {"status": "PASS" if passed else status,
                                             "output": output}
                save()
                print(f"baseline/{model}: {'PASS' if passed else status}", flush=True)
                if not passed:
                    print(output[-3000:])
                    return 2
            for mutant in selected:
                unit, model, needle, replacement = MUTANTS[mutant]
                started = time.monotonic()
                try:
                    header = unit.startswith("include/")
                    path = ROOT / unit if header else ROOT / "src" / f"{unit}.cpp"
                    changed = replace_once(path.read_text(), needle, replacement)
                    mutated_objects = dict(objects)
                    test = tests[model]
                    if header:
                        # Recompile every TU against one consistent header overlay;
                        # mixing old/new inline definitions would violate the ODR.
                        overlay = directory / mutant
                        target = overlay / unit
                        target.parent.mkdir(parents=True)
                        target.write_text(changed)
                        mutation_flags = ["-I", str(overlay / "include"), *flags]
                        for name in RUNTIME:
                            obj = overlay / f"{name}.o"
                            build(parsed.cxx, mutation_flags, ROOT / "src" / f"{name}.cpp", obj)
                            mutated_objects[name] = obj
                        test = overlay / "test.o"
                        build(parsed.cxx, mutation_flags, ROOT / "tests" / MODELS[model], test)
                    else:
                        source = directory / f"{mutant}.cpp"
                        source.write_text(changed)
                        obj = directory / f"{mutant}.o"
                        build(parsed.cxx, flags, source, obj)
                        mutated_objects[unit] = obj
                    binary = directory / f"run-{mutant}"
                    link(mutated_objects, test, binary)
                    status, output = execute(binary, parsed.seed, parsed.cases,
                                             parsed.steps, parsed.timeout)
                except (ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
                    status, output = "BUILD_ERROR", str(exc)
                report["mutants"][mutant] = {
                    "status": status, "model": model, "source": unit,
                    "seconds": round(time.monotonic() - started, 3), "output": output}
                save()
                print(f"{mutant}: {status}", flush=True)
                if status != "SURVIVED":
                    print("  " + "\n  ".join(output.strip().splitlines()[-5:]), flush=True)
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        report["infrastructure_error"] = str(exc)
        save()
        print(f"Campaign infrastructure failure: {exc}", file=sys.stderr)
        return 2
    statuses = [item["status"] for item in report["mutants"].values()]
    detected = sum(status.startswith("KILLED(") for status in statuses)
    print(f"Protocol mutations: {detected}/{len(selected)} assertion-detected; "
          f"{statuses.count('SURVIVED')} survived; {statuses.count('TIMEOUT')} timed out; "
          f"{sum(s.startswith('CRASH') for s in statuses)} crashed; "
          f"{statuses.count('BUILD_ERROR')} build errors")
    print(f"Report: {parsed.report}")
    return 0 if detected == len(selected) else 1


if __name__ == "__main__":
    sys.exit(main())
