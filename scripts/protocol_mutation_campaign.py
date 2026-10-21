#!/usr/bin/env python3
"""Mutation test TaskGraph and Completion against their actual C++ property models.

All source variants and objects are created under a disposable temp directory.
The checked-out runtime is never patched. Only an assertion-based property
failure counts as an identified mutant; crashes, timeouts and compile errors
remain distinct outcomes, not successful kills.
"""
from __future__ import annotations

import argparse
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


def run(cmd: list[str], *, timeout: int, env: dict[str, str] | None = None):
    return subprocess.run(cmd, env=env, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout)


def build(cxx: str, args: list[str], source: Path, output: Path) -> None:
    process = run([cxx, *args, "-c", str(source), "-o", str(output)], timeout=120)
    if process.returncode:
        raise RuntimeError(f"compiling {source.name}:\n{process.stdout[-3500:]}")


def execute(binary: Path, seed: str, cases: int, steps: int, timeout: int):
    env = dict(os.environ, DAGFLOW_PROPERTY_SEED=seed,
               DAGFLOW_PROPERTY_CASES=str(cases), DAGFLOW_PROPERTY_STEPS=str(steps))
    try:
        proc = run([str(binary)], env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        return "TIMEOUT", ""
    if proc.returncode == 0:
        return "PASS", proc.stdout
    # The existing PBT runner emits this distinctive line only on a property
    # violation, after shrinking. A SIGSEGV/abort with no property report
    # must not inflate the assertion-detected mutation score.
    if "property failed:" in proc.stdout:
        return "KILLED(property)", proc.stdout
    return f"CRASH(exit={proc.returncode})", proc.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cxx", default=os.environ.get("CXX") or shutil.which("c++"))
    parser.add_argument("--seed", default="496833221358")
    parser.add_argument("--cases", type=int, default=48)
    parser.add_argument("--steps", type=int, default=48)
    parser.add_argument("--timeout", type=int, default=20)
    parsed = parser.parse_args()
    if not parsed.cxx:
        parser.error("C++ compiler unavailable")
    flags = ["-std=c++23", "-O0", "-g0", "-pthread", "-DDAGFLOW_STATIC",
             "-I", str(ROOT / "include"), "-I", str(ROOT / "tests")]
    results = {}
    try:
        with tempfile.TemporaryDirectory(prefix="dagflow-protocol-mutants-") as temp:
            directory = Path(temp)
            originals = {name: (ROOT / "src" / f"{name}.cpp").read_text()
                         for name in ("task_graph", "thread_pool")}
            shared = {}
            for name in RUNTIME:
                if name in originals:
                    continue
                obj = directory / f"{name}.o"
                build(parsed.cxx, flags, ROOT / "src" / f"{name}.cpp", obj)
                shared[name] = obj
            tests = {}
            for name, src in MODELS.items():
                obj = directory / f"{name}_model.o"
                build(parsed.cxx, flags, ROOT / "tests" / src, obj)
                tests[name] = obj
            # Baseline first: a valid property must pass in this compilation.
            for testname in MODELS:
                objects = dict(shared)
                for name in originals:
                    obj = directory / f"{name}_baseline.o"
                    if not obj.exists():
                        build(parsed.cxx, flags, ROOT / "src" / f"{name}.cpp", obj)
                    objects[name] = obj
                binary = directory / f"baseline_{testname}"
                link = run([parsed.cxx, "-pthread", *map(str, objects.values()),
                            str(tests[testname]), "-o", str(binary)], timeout=80)
                if link.returncode:
                    raise RuntimeError(f"linking baseline {testname}: {link.stdout[-3000:]}")
                status, output = execute(binary, parsed.seed, parsed.cases,
                                         parsed.steps, parsed.timeout)
                print(f"baseline/{testname}: {status}", flush=True)
                if status != "PASS":
                    print(output[-2200:])
                    return 2
            for mutant, (name, model, needle, replacement) in MUTANTS.items():
                changed = replace_once(originals[name], needle, replacement)
                unit = directory / f"{mutant}.cpp"
                unit.write_text(changed)
                mutant_obj = directory / f"{mutant}.o"
                build(parsed.cxx, flags, unit, mutant_obj)
                objects = [mutant_obj if item == name else directory / f"{item}_baseline.o"
                           for item in RUNTIME if item not in shared]
                objects += list(shared.values())
                binary = directory / mutant
                link = run([parsed.cxx, "-pthread", *map(str, objects),
                            str(tests[model]), "-o", str(binary)], timeout=80)
                if link.returncode:
                    raise RuntimeError(f"linking {mutant}: {link.stdout[-3000:]}")
                status, output = execute(binary, parsed.seed, parsed.cases,
                                         parsed.steps, parsed.timeout)
                results[mutant] = status
                print(f"{mutant}: {status}", flush=True)
                if status != "PASS":
                    print("  " + "\n  ".join(output.strip().splitlines()[-5:]), flush=True)
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        print(f"Campaign infrastructure failure: {exc}", file=sys.stderr)
        return 2
    detected = sum(v == "KILLED(property)" for v in results.values())
    print(f"Protocol mutations: {detected}/{len(MUTANTS)} assertion-detected; "
          f"{sum(v == 'PASS' for v in results.values())} survived; "
          f"{sum(v == 'TIMEOUT' for v in results.values())} timed out; "
          f"{sum(v.startswith('CRASH') for v in results.values())} crashed")
    return 0 if detected == len(MUTANTS) else 1


if __name__ == "__main__":
    sys.exit(main())
