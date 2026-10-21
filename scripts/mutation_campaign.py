#!/usr/bin/env python3
"""Test-only, disposable C++ mutants for the bounded queue history oracle.

Builds and runs the *actual C++ queue implementation* through the concurrent
history PBT executable, once as baseline and once for each isolated mutation.
Never edits sources or a working-tree file. Mutant timeouts are reported
separately from assertion failures; the latter give the strongest evidence.
"""
from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
HEADER = pathlib.Path("dagflow/detail/ring_mpmc.hpp")
SOURCE = ROOT / "tests/concurrent_history_property_tests.cpp"


def mutate_lost_published_payload(source: str) -> str:
    old = "cell.value = std::forward<U>(value);"
    assert source.count(old) == 1, "expected exactly one push payload assignment"
    return source.replace(old, "cell.value = value_type{};", 1)


def mutate_lost_acquired_payload(source: str) -> str:
    old = "value = std::move(cell.value);"
    assert source.count(old) == 1, "expected exactly one pop payload assignment"
    return source.replace(old, "value = value_type{};", 1)


def mutate_false_full_admission(source: str) -> str:
    anchor = "bool push_impl(U&& value) noexcept {"
    assert source.count(anchor) == 1
    before, after = source.split(anchor)
    old = "} else if (difference < 0) {\n        return false;"
    assert after.count(old) == 1
    return before + anchor + after.replace(old, "} else if (difference < 0) {\n        return true;", 1)


def mutate_false_successful_pop(source: str) -> str:
    old = "cell.sequence.store(position + N, std::memory_order_release);\n    return true;"
    assert source.count(old) == 1
    return source.replace(old, "cell.sequence.store(position + N, std::memory_order_release);\n    return false;", 1)


MUTANTS = {
    "discard-push-payload": mutate_lost_published_payload,
    "discard-pop-payload": mutate_lost_acquired_payload,
    "lie-about-full-push": mutate_false_full_admission,
    "lie-about-successful-pop": mutate_false_successful_pop,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cxx", default=os.environ.get("CXX") or shutil.which("c++"))
    parser.add_argument("--seed", default="496833221358")
    parser.add_argument("--cases", type=int, default=32)
    parser.add_argument("--steps", type=int, default=36)
    parser.add_argument("--timeout", type=int, default=20,
                        help="per-mutant execution timeout in seconds")
    args = parser.parse_args()
    if not args.cxx:
        parser.error("C++ compiler not found; specify --cxx")
    source = (ROOT / "include" / HEADER).read_text()
    results: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="dagflow-mutations-") as temp:
        directory = pathlib.Path(temp)
        for name, variant in [("baseline", source)] + [
            (name, mutate(source)) for name, mutate in MUTANTS.items()
        ]:
            workspace = directory / name
            fake_header = workspace / "include" / HEADER
            fake_header.parent.mkdir(parents=True)
            fake_header.write_text(variant)
            executable = workspace / "history_tests"
            compile_cmd = [args.cxx, "-std=c++23", "-O0", "-pthread",
                           "-I", str(workspace / "include"),
                           "-I", str(ROOT / "include"),
                           "-I", str(ROOT / "tests"), str(SOURCE),
                           "-o", str(executable)]
            compiled = subprocess.run(compile_cmd, text=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, timeout=90)
            if compiled.returncode:
                print(f"{name}: COMPILE_ERROR\n{compiled.stdout[-2000:]}")
                return 2
            env = dict(os.environ, DAGFLOW_PROPERTY_SEED=args.seed,
                       DAGFLOW_PROPERTY_CASES=str(args.cases),
                       DAGFLOW_PROPERTY_STEPS=str(args.steps))
            try:
                completed = subprocess.run([str(executable)], env=env, text=True,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT,
                                           timeout=args.timeout)
                status = "PASS" if completed.returncode == 0 else "KILLED(assertion)"
                if completed.returncode and name != "baseline":
                    print(f"  {name} counterexample: {completed.stdout[-550:].strip()}")
            except subprocess.TimeoutExpired:
                status = "TIMEOUT"  # Not counted as assertion-detected.
            print(f"{name}: {status}")
            results[name] = status
    if results["baseline"] != "PASS":
        print("Baseline did not pass; mutation score is invalid.", file=sys.stderr)
        return 2
    killed = sum(status == "KILLED(assertion)" for n, status in results.items()
                 if n != "baseline")
    timed_out = sum(status == "TIMEOUT" for n, status in results.items()
                    if n != "baseline")
    survived = len(MUTANTS) - killed - timed_out
    print(f"Mutation result: {killed}/{len(MUTANTS)} assertion-killed, "
          f"{timed_out} timeout, {survived} survived")
    return 0 if not survived and not timed_out else 1


if __name__ == "__main__":
    sys.exit(main())
