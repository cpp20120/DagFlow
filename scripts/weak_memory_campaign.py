#!/usr/bin/env python3
"""Check source-derived deque/parking/completion/idle/shutdown models with Relacy.

Requires clean baselines and explicit counterexamples for negative controls.
Documented survivor controls are reported separately, never as detected mutants.
No automatic downloads or production source edits. Each adapter, source hash,
checker result and counterexample history is retained in the report directory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

from weak_memory_cases import ROOT, SOURCES, cases


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--relacy", type=Path, required=True,
                        help="directory containing relacy/relacy.hpp")
    parser.add_argument("--cxx", default="c++")
    parser.add_argument("--protocol", choices=SOURCES, action="append")
    parser.add_argument("--mutant", action="append", help="restrict mutants; baselines always run")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--report", type=Path,
                        default=ROOT / "out/weak-memory/report.json")
    args = parser.parse_args()
    if args.timeout < 1 or not (args.relacy / "relacy/relacy.hpp").is_file():
        parser.error("positive timeout and a Relacy include root are required")
    protocols = list(dict.fromkeys(args.protocol or SOURCES))
    report = {"compiler": args.cxx, "search": "per-check",
              "execution_depth_limit": 200, "sources": {}, "checks": {}}
    args.report.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.report.write_text(json.dumps(report, indent=2) + "\n")

    def run(command):
        return subprocess.run(command, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=args.timeout)

    try:
        variants = {p: list(cases(p)) for p in protocols}
        available = {name for items in variants.values() for name, _, _ in items}
        if args.mutant and set(args.mutant) - (available - {"baseline"}):
            parser.error("unknown mutant or mutant excluded by --protocol")
        try:
            revision = run(["git", "-C", str(args.relacy), "rev-parse", "HEAD"])
            report["relacy_revision"] = revision.stdout.strip() if revision.returncode == 0 else "unknown"
        except FileNotFoundError:
            report["relacy_revision"] = "unknown"
        report["compiler_version"] = run([args.cxx, "--version"]).stdout
        for path in {path for p in protocols for path in SOURCES[p]} | {
                f"tests/weak_memory/{p}_relacy_tests.cpp" for p in protocols} | {
                "tests/weak_memory/model_support.hpp", "tests/weak_memory/parking_fixture.hpp",
                "tests/weak_memory/idle_accounting_fixture.hpp",
                "tests/weak_memory/shutdown_fixture.hpp",
                "scripts/weak_memory_adapters.py", "scripts/weak_memory_cases.py",
                "scripts/weak_memory_campaign.py"}:
            report["sources"][path] = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        save()
        with tempfile.TemporaryDirectory(prefix="dagflow-weak-memory-") as temp:
            directory = Path(temp)
            for protocol, items in variants.items():
                for name, header, suites in items:
                    if name != "baseline" and args.mutant and name not in args.mutant:
                        continue
                    (directory / f"{protocol}_under_test.hpp").write_text(header)
                    artifact = args.report.parent / f"{protocol}-{name}.hpp"
                    artifact.write_text(header)
                    binary = directory / f"{protocol}-{name}"
                    print(f"building {protocol}/{name}", flush=True)
                    built = run([args.cxx, "-std=c++23", "-O2", "-pthread",
                                 "-I", str(args.relacy.resolve()),
                                 "-I", str(directory), "-I", str(ROOT / "include"),
                                 "-I", str(ROOT / "tests/weak_memory"),
                                 str(ROOT / f"tests/weak_memory/{protocol}_relacy_tests.cpp"),
                                 "-o", str(binary)])
                    if built.returncode:
                        report["build_error"] = {"variant": f"{protocol}/{name}", "output": built.stdout}
                        save()
                        print(built.stdout)
                        return 2
                    for suite, expected in suites:
                        key = f"{protocol}/{name}/{suite}"
                        kind = ("baseline" if name == "baseline" else
                                "survivor-control" if expected == ("SUCCESS",) else "counterexample")
                        print(f"checking {key}", flush=True)
                        result = run([str(binary), suite])
                        match = re.search(rf"^WEAK_MEMORY_RESULT {re.escape(suite)} (.+) executions=(\d+)$",
                                          result.stdout, re.M)
                        status = match[1] if match else "NO_CHECKER_RESULT"
                        config = re.search(
                            rf"^WEAK_MEMORY_CONFIG {re.escape(suite)} search=(sched_full|sched_bound) "
                            r"context_bound=(\d+) depth=(\d+)$", result.stdout, re.M)
                        code = {"SUCCESS": 0, "USER ASSERT FAILED": 86,
                                "DATA RACE": 86, "DEADLOCK": 87}.get(status)
                        passed = bool(config) and status in expected and result.returncode == code
                        report["checks"][key] = {
                            "kind": kind,
                            "matched_expectation": passed, "status": status,
                            "expected": expected, "exit_code": result.returncode,
                            "executions": int(match[2]) if match else None,
                            "search": config[1] if config else None,
                            "context_bound": int(config[2]) if config else None,
                            "execution_depth_limit": int(config[3]) if config else None,
                            "adapter": artifact.name, "output": result.stdout}
                        save()
                        print(f"{key}: {'PASS' if passed else 'FAIL'} ({kind}: {status})", flush=True)
                        if not passed:
                            print(result.stdout)
                            return 1
    except (ValueError, OSError, subprocess.TimeoutExpired) as error:
        report["infrastructure_error"] = str(error)
        save()
        print(error)
        return 2
    print(f"Report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
