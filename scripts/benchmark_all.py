#!/usr/bin/env python3
"""Run DagFlow's Pool/graph, stress, and DagFlow-vs-oneTBB benchmark suites."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def csv_ints(value: str) -> list[int]:
    try:
        values = list(dict.fromkeys(int(part) for part in value.split(",")))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or min(values) < 0:
        raise argparse.ArgumentTypeError("values must be nonnegative")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path,
                        default=ROOT / "out" / "benchmarks" / ("complete-" + dt.datetime.now().strftime("%Y%m%d-%H%M%S")))
    parser.add_argument("--suites", default="runtime,stress,api",
                        help="comma-separated: runtime,stress,api")
    parser.add_argument("--allocator", choices=("system", "mimalloc", "tbbmalloc"), default="system")
    parser.add_argument("--compiler", default="clang++")
    parser.add_argument("--workers", type=csv_ints, default=[1, 2, 4, 8])
    parser.add_argument("--tasks", type=int, default=4096)
    parser.add_argument("--work-ns", type=csv_ints, default=[0, 1000])
    parser.add_argument("--repeats", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--api-runs", type=int, default=5)
    parser.add_argument("--api-warmup", type=int, default=1)
    parser.add_argument("--stress-rounds", type=int, default=3)
    parser.add_argument("--stress-repeats", type=int, default=9)
    parser.add_argument("--stress-profiles", default="o3,o3-lto")
    parser.add_argument("--runtime-profiles", default="release,lto")
    parser.add_argument("--affinity", choices=("physical", "inherit"), default="inherit")
    parser.add_argument("--jobs", type=int, default=min(4, os.cpu_count() or 1))
    args = parser.parse_args()
    suites = list(dict.fromkeys(args.suites.split(",")))
    if not suites or set(suites) - {"runtime", "stress", "api"}:
        parser.error("--suites must contain runtime, stress, and/or api")
    args.suites = suites
    if (min(args.workers) < 1 or args.tasks < 1 or args.repeats < 1 or args.warmup < 0 or
            args.api_runs < 1 or args.api_warmup < 0 or args.stress_rounds < 1 or
            args.stress_repeats < 1 or args.jobs < 1):
        parser.error("invalid worker, task, repeat, or job count")
    return args


class Runner:
    def __init__(self, output: Path):
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.logs = self.output / "logs"
        self.logs.mkdir()
        self.commands: list[dict] = []

    def run(self, label: str, command: list[object]) -> None:
        argv = list(map(str, command))
        print(f"[{label}] {shlex.join(argv)}", flush=True)
        started = time.time()
        record = {"label": label, "argv": argv, "started_unix": started}
        self.commands.append(record)
        try:
            result = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True)
            (self.logs / f"{label}.stdout").write_text(result.stdout)
            (self.logs / f"{label}.stderr").write_text(result.stderr)
            record["returncode"] = result.returncode
            if result.returncode:
                raise RuntimeError(f"{label} failed; inspect {self.logs / (label + '.stderr')}")
        finally:
            record["elapsed_seconds"] = time.time() - started
            (self.output / "commands.json").write_text(json.dumps(self.commands, indent=2) + "\n")


def make_stress_cases(workers: list[int], tasks: int) -> list[dict]:
    cases: list[dict] = []
    max_workers = max(workers)
    for count in workers:
        cases.append(dict(scenario="external-contention", workers=count,
                          producers=min(2, count), shards=count, tasks=tasks,
                          submit_batch=0, iterations=0))
    for producers in (1, 2, 4):
        for batch in (0, 16, 64):
            for shards in sorted({1, max_workers}):
                cases.append(dict(scenario="external-contention", workers=max_workers,
                                  producers=producers, shards=shards, tasks=tasks,
                                  submit_batch=batch, iterations=0))
    for scenario in ("hot-shard-skew", "local-overflow", "nested-helping", "mixed-chaos"):
        for count in sorted({1, min(4, max_workers), max_workers}):
            cases.append(dict(scenario=scenario, workers=count,
                              producers=2 if scenario == "mixed-chaos" else 1,
                              shards=count, tasks=tasks, iterations=0))
    for count in sorted({1, max_workers}):
        cases.append(dict(scenario="idle-burst", workers=count, producers=1,
                          shards=count, tasks=64, iterations=0))
    unique = []
    for case in cases:
        if case not in unique:
            unique.append(case)
    return unique


def main() -> int:
    try:
        args = parse_args()
        runner = Runner(args.output)
        manifest = {"schema": 1, "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "options": {key: str(value) if isinstance(value, Path) else value
                                for key, value in vars(args).items()}}
        (runner.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        workers = ",".join(map(str, args.workers))

        if "runtime" in args.suites:
            runner.run("runtime-suite", [sys.executable, ROOT / "scripts" / "benchmark_suite.py",
                        "--profiles", args.runtime_profiles, "--allocator", args.allocator,
                        "--compiler", args.compiler, "--workers", workers,
                        "--tasks", args.tasks, "--work-ns", ",".join(map(str, args.work_ns)),
                        "--repeats", args.repeats, "--warmup", args.warmup,
                        "--scenarios", "all", "--modes", "throughput,latency",
                        "--jobs", args.jobs, "--output", runner.output / "runtime"])

        if "stress" in args.suites:
            cases_path = runner.output / "stress-cases.json"
            cases_path.write_text(json.dumps(make_stress_cases(args.workers, args.tasks), indent=2) + "\n")
            runner.run("stress-harness", [sys.executable, ROOT / "scripts" / "benchmark_main.py",
                        "--out", runner.output / "stress", "--cases", cases_path,
                        "--profiles", args.stress_profiles, "--allocator", args.allocator,
                        "--compiler", args.compiler, "--rounds", args.stress_rounds,
                        "--repeats", args.stress_repeats, "--warmup", args.warmup,
                        "--min-ms", 200, "--perf", "off", "--affinity", args.affinity])

        if "api" in args.suites:
            dagflow_out = runner.output / "public-api" / "dagflow"
            tbb_out = runner.output / "public-api" / "tbb"
            common = ["--allocator", args.allocator, "--compiler", args.compiler,
                      "--workers", max(args.workers), "--runs", args.api_runs,
                      "--warmup", args.api_warmup]
            runner.run("dagflow-public-api", [sys.executable, ROOT / "scripts" / "benchmark_public_api.py",
                        *common, "--output", dagflow_out])
            runner.run("tbb-public-api", [sys.executable, ROOT / "scripts" / "benchmar_tbb.py",
                        "--compiler", args.compiler, "--workers", max(args.workers),
                        "--runs", args.api_runs, "--warmup", args.api_warmup,
                        "--output", tbb_out, "--dagflow-results", dagflow_out / "results.json"])

        print(f"Benchmark suites completed. Results: {runner.output}")
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"complete benchmark runner: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
