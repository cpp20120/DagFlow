#!/usr/bin/env python3
"""Build and run a oneTBB benchmark aligned with DagFlow's public-API shapes.

The default suite intentionally excludes the old "batched independent" TBB test:
that test grouped ten user jobs into one TBB scheduler task, while DagFlow's
submit_batch_detached still publishes ten logical scheduler tasks. It is not an
apples-to-apples API comparison.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil

from benchmark_build import commands as cmake_commands
import subprocess
import sys

BENCHMARKS = ("chain", "independent", "parallel-for", "workflow", "noop")


def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve()
    repo = here.parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helper", type=Path,
                        default=repo / "bench" / "tbb_bench.cpp")
    parser.add_argument("--compiler", default="clang++")
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--payload-rounds", type=int, default=100)
    parser.add_argument("--seed", type=lambda x: int(x, 0), default=0x9E3779B9)
    parser.add_argument("--benchmarks", default=",".join(BENCHMARKS))
    parser.add_argument("--native", action="store_true")
    parser.add_argument("--lto", action="store_true")
    parser.add_argument("--cxxflags", default="")
    parser.add_argument("--cpus", help="Linux CPU list, e.g. 0,2,4,6")
    parser.add_argument("--output", type=Path,
                        default=repo / "out" / "tbb-public-api-benchmark")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--build-only", action="store_true",
                        help="compile and record build metadata without running benchmarks")
    parser.add_argument("--dagflow-results", type=Path,
                        help="optional DagFlow results.json for a side-by-side report")
    args = parser.parse_args()
    if args.workers < 1 or args.runs < 1 or args.warmup < 0 or args.payload_rounds < 1:
        parser.error("workers/runs/payload-rounds must be positive; warmup must be nonnegative")
    args.benchmarks = list(dict.fromkeys(x.strip() for x in args.benchmarks.split(",") if x.strip()))
    unknown = [x for x in args.benchmarks if x not in BENCHMARKS]
    if unknown:
        parser.error(f"unknown benchmarks: {', '.join(unknown)}")
    return args


def run(command: list[str], *, affinity: set[int] | None = None,
        timeout: float = 900.0, check: bool = True) -> subprocess.CompletedProcess[str]:
    def set_affinity() -> None:
        if affinity is not None:
            os.sched_setaffinity(0, affinity)

    print("+", shlex.join(map(str, command)), file=sys.stderr)
    return subprocess.run(
        [str(x) for x in command], text=True, capture_output=True,
        check=check, timeout=timeout,
        preexec_fn=set_affinity if affinity is not None and hasattr(os, "sched_setaffinity") else None,
    )


def parse_cpu_list(text: str | None) -> set[int] | None:
    if not text:
        return None
    cpus: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError("empty CPU item")
        if "-" in part:
            lo_s, hi_s = part.split("-", 1)
            lo, hi = int(lo_s), int(hi_s)
            if lo > hi:
                raise ValueError(f"invalid CPU range: {part}")
            cpus.update(range(lo, hi + 1))
        else:
            cpus.add(int(part))
    return cpus


def pkg_config_version() -> str | None:
    try:
        p = run(["pkg-config", "--modversion", "tbb"], check=False)
        return p.stdout.strip() or None if p.returncode == 0 else None
    except OSError:
        return None


def build(args: argparse.Namespace, output: Path) -> tuple[Path, dict]:
    helper = args.helper.resolve()
    if not helper.is_file():
        raise RuntimeError(f"missing helper: {helper}")
    build_dir = output / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    binary = build_dir / "tbb-public-api-bench"

    repo = Path(__file__).resolve().parents[1]
    profile = "o3-lto" if args.lto else "o3"
    commands = cmake_commands(repo, build_dir / "cmake", args.compiler, profile,
                             ["dagflow_tbb_bench"],
                             {"DAGFLOW_BUILD_BENCH": "ON", "DAGFLOW_TBB_BENCH_SOURCE": str(helper),
                              "DAGFLOW_ENABLE_NATIVE": "ON" if args.native else "OFF",
                              "CMAKE_CXX_FLAGS": args.cxxflags,
                              "DAGFLOW_USE_LLD": "ON" if "clang" in args.compiler else "OFF"})
    if not args.no_build:
        try:
            for command in commands:
                run(command)
            shutil.copy2(build_dir / "cmake" / "dagflow-tbb-bench", binary)
        except subprocess.CalledProcessError as error:
            sys.stderr.write(error.stdout or "")
            sys.stderr.write(error.stderr or "")
            raise RuntimeError(
                "oneTBB benchmark build failed; install oneTBB development headers/library "
                "(for example package 'tbb'/'libtbb-dev')"
            ) from error
    elif not binary.is_file():
        raise RuntimeError("--no-build requested but benchmark binary is missing")

    compiler_version = run([args.compiler, "--version"]).stdout.splitlines()[0]
    manifest = {
        "schema": 1,
        "runtime": "oneTBB",
        "platform": platform.platform(),
        "python": sys.version,
        "compiler": compiler_version,
        "tbb_version": pkg_config_version(),
        "workers": args.workers,
        "runs": args.runs,
        "warmup": args.warmup,
        "payload_rounds": args.payload_rounds,
        "seed": args.seed,
        "native": args.native,
        "lto": args.lto,
        "extra_cxxflags": args.cxxflags,
        "source_sha256": hashlib.sha256(helper.read_bytes()).hexdigest(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "profile": profile,
        "build_commands": commands,
    }
    return binary, manifest


def invoke(binary: Path, name: str, args: argparse.Namespace,
           affinity: set[int] | None) -> dict:
    command = [
        binary,
        "--benchmark", name,
        "--workers", str(args.workers),
        "--runs", str(args.runs),
        "--warmup", str(args.warmup),
        "--payload-rounds", str(args.payload_rounds),
        "--seed", str(args.seed),
    ]
    p = run(command, affinity=affinity)
    if p.stderr:
        print(p.stderr, end="", file=sys.stderr)
    lines = [x for x in p.stdout.splitlines() if x.strip()]
    if len(lines) != 1:
        raise RuntimeError(f"expected one JSON line from {name}, got {len(lines)}")
    return json.loads(lines[0])


def format_time(seconds: float) -> str:
    if seconds >= 1.0:
        return f"{seconds:.3f} s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.3f} ms"
    if seconds >= 1e-6:
        return f"{seconds * 1e6:.1f} µs"
    return f"{seconds * 1e9:.1f} ns"


def format_rate(value: float, unit: str) -> str:
    suffix = "task/s" if unit == "task" else "elem/s"
    if value >= 1e9:
        return f"{value / 1e9:.3f} G {suffix}"
    if value >= 1e6:
        return f"{value / 1e6:.3f} M {suffix}"
    if value >= 1e3:
        return f"{value / 1e3:.3f} k {suffix}"
    return f"{value:.1f} {suffix}"


def make_markdown(rows: list[dict], manifest: dict) -> str:
    lines = [
        "# oneTBB public API benchmark",
        "",
        "`workers` is the arena concurrency limit. With `global_control(max_allowed_parallelism=N)`, "
        "oneTBB can use at most N-1 scheduler worker threads while the application thread may occupy "
        "the remaining execution slot; the maximum simultaneous task execution is N.",
        "",
        "The old TBB 'batched independent' row is intentionally omitted: grouping ten user jobs into "
        "one `task_group` task changes the number of scheduler tasks and is not equivalent to "
        "DagFlow `submit_batch_detached`.",
        "",
        f"Concurrency: **{manifest['workers']}** · oneTBB: **{manifest['tbb_version'] or 'unknown'}** · "
        f"compiler: `{manifest['compiler']}` · LTO: **{manifest['lto']}** · "
        f"`-march=native`: **{manifest['native']}**",
        "",
        "| Benchmark | Runs | Mean | Median | Min | Max | Throughput | Mean / unit |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['benchmark']} | {row['runs']} | {format_time(row['mean_s'])} | "
            f"{format_time(row['median_s'])} | {format_time(row['min_s'])} | "
            f"{format_time(row['max_s'])} | {format_rate(row['throughput_per_s'], row['unit'])} | "
            f"{row['mean_ns_per_unit']:.1f} ns/{row['unit']} |"
        )
    lines += [
        "",
        "Setup/reset and validation are outside the timed interval. Chain/workflow graph construction "
        "is intentionally inside the timed interval, matching the DagFlow public-API benchmark's "
        "complete-operation timing boundary.",
        "",
    ]
    return "\n".join(lines)


def load_dagflow(path: Path) -> dict[str, dict]:
    rows = json.loads(path.read_text())
    label_to_key = {
        "Dependent chain (1,000 tasks)": "chain",
        "Independent tasks (1,000)": "independent",
        "Parallel_for (1,000,000 elements)": "parallel-for",
        "Workflow (width=10, depth=5)": "workflow",
        "Noop tasks (1,000,000)": "noop",
    }
    result: dict[str, dict] = {}
    for row in rows:
        key = row.get("key") or label_to_key.get(row.get("benchmark"))
        if key:
            result[key] = row
    return result


def make_comparison(tbb_rows: list[dict], dagflow: dict[str, dict]) -> str:
    lines = [
        "# DagFlow vs oneTBB",
        "",
        "`TBB/DagFlow time` is `oneTBB mean / DagFlow mean`; values above 1 mean DagFlow "
        "completed the same workload faster. Compare only runs built with matching flags, CPU set, "
        "payload, and timing boundaries.",
        "",
        "| Benchmark | DagFlow mean | oneTBB mean | DagFlow throughput | oneTBB throughput | TBB/DagFlow time |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for tbb_row in tbb_rows:
        key = tbb_row["key"]
        d = dagflow.get(key)
        if not d:
            continue
        ratio = tbb_row["mean_s"] / d["mean_s"]
        lines.append(
            f"| {tbb_row['benchmark']} | {format_time(d['mean_s'])} | {format_time(tbb_row['mean_s'])} | "
            f"{format_rate(d['throughput_per_s'], d['unit'])} | "
            f"{format_rate(tbb_row['throughput_per_s'], tbb_row['unit'])} | {ratio:.3f}× |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    try:
        args = parse_args()
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=True)
        affinity = parse_cpu_list(args.cpus)
        if affinity is not None and not hasattr(os, "sched_setaffinity"):
            raise RuntimeError("--cpus requires Linux sched_setaffinity")

        binary, manifest = build(args, output)
        manifest["benchmarks"] = args.benchmarks
        manifest["cpus"] = sorted(affinity) if affinity is not None else None
        if affinity is None and hasattr(os, "sched_getaffinity"):
            manifest["allowed_cpus"] = sorted(os.sched_getaffinity(0))

        if args.build_only:
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(f"oneTBB benchmark binary built successfully in {output / 'build'}")
            return 0

        rows = [invoke(binary, name, args, affinity) for name in args.benchmarks]
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n")

        fieldnames = sorted({k for row in rows for k in row if k != "samples_s"})
        with (output / "results.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows({k: v for k, v in row.items() if k != "samples_s"} for row in rows)

        report = make_markdown(rows, manifest)
        (output / "report.md").write_text(report + "\n")
        print(report)

        if args.dagflow_results:
            comparison = make_comparison(rows, load_dagflow(args.dagflow_results))
            (output / "comparison.md").write_text(comparison + "\n")
            print("\n" + comparison)

        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"oneTBB public API benchmark: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
