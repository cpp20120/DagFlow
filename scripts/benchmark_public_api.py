#!/usr/bin/env python3
"""Build and run a user-facing DagFlow public-API benchmark.

The workload code uses the public DagFlow API.  Timing is collected from a
normal Release build.  Allocation counts are collected in a second diagnostic
build because DAGFLOW_RUNTIME_DIAGNOSTICS deliberately adds atomic overhead and
must not be used as a timing baseline.
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
import time

BENCHMARKS = (
    "chain",
    "independent",
    "independent-batch",
    "parallel-for",
    "workflow",
    "noop",
)

def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve()
    default_repo = here.parents[1] if here.parent.name == "scripts" else Path.cwd()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=default_repo,
                        help="DagFlow source tree (default: repository containing this script)")
    parser.add_argument("--helper", type=Path,
                        help="public_api_bench.cpp; default: bench/public_api_bench.cpp in repo")
    parser.add_argument("--compiler", default="clang++")
    parser.add_argument("--allocator", choices=("system", "mimalloc", "tbbmalloc"),
                        default="mimalloc")
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--alloc-runs", type=int, default=1,
                        help="diagnostic runs used only for allocation counts")
    parser.add_argument("--alloc-warmup", type=int, default=1)
    parser.add_argument("--benchmarks", default=",".join(BENCHMARKS),
                        help="comma-separated benchmark names")
    parser.add_argument("--native", action="store_true", help="add -march=native")
    parser.add_argument("--lto", action="store_true", help="add -flto")
    parser.add_argument("--cxxflags", default="", help="extra compiler/linker flags")
    parser.add_argument("--cpus", help="Linux CPU list, e.g. 0,2,4,6; inherited by workers")
    parser.add_argument("--output", type=Path,
                        default=default_repo / "out" / "public-api-benchmark")
    parser.add_argument("--no-build", action="store_true",
                        help="reuse binaries already present in --output/build")
    parser.add_argument("--build-only", action="store_true",
                        help="compile and record build metadata without running benchmarks")
    args = parser.parse_args()

    if args.workers < 1 or args.runs < 1 or args.warmup < 0 or args.alloc_runs < 1 or args.alloc_warmup < 0:
        parser.error("workers/runs must be positive; warmup counts must be nonnegative")
    args.benchmarks = list(dict.fromkeys(x.strip() for x in args.benchmarks.split(",") if x.strip()))
    unknown = [x for x in args.benchmarks if x not in BENCHMARKS]
    if unknown:
        parser.error(f"unknown benchmarks: {', '.join(unknown)}")
    return args


def run(command: list[str], *, cwd: Path | None = None, affinity: set[int] | None = None,
        timeout: float = 600.0) -> subprocess.CompletedProcess[str]:
    def set_affinity() -> None:
        if affinity is not None:
            os.sched_setaffinity(0, affinity)

    print("+", shlex.join(map(str, command)), file=sys.stderr)
    return subprocess.run(
        [str(x) for x in command], cwd=cwd, text=True, capture_output=True,
        check=True, timeout=timeout,
        preexec_fn=set_affinity if affinity is not None and hasattr(os, "sched_setaffinity") else None,
    )


def parse_cpu_list(text: str | None) -> set[int] | None:
    if not text:
        return None
    cpus: set[int] = set()
    for item in text.split(","):
        item = item.strip()
        if not item:
            raise ValueError("empty CPU item")
        if "-" in item:
            lo_s, hi_s = item.split("-", 1)
            lo, hi = int(lo_s), int(hi_s)
            if lo > hi:
                raise ValueError(f"invalid CPU range: {item}")
            cpus.update(range(lo, hi + 1))
        else:
            cpus.add(int(item))
    if not cpus:
        raise ValueError("empty CPU set")
    return cpus


def source_hash(paths: list[Path]) -> str:
    h = hashlib.sha256()
    for path in sorted(paths):
        h.update(str(path).encode())
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def build(args: argparse.Namespace, helper: Path, repo: Path, output: Path) -> tuple[Path, Path, dict]:
    build_dir = output / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    timing = build_dir / "dagflow-public-api-bench"
    diagnostic = build_dir / "dagflow-public-api-bench-diagnostics"

    runtime = sorted((repo / "src").glob("*.cpp"))
    missing = [p for p in [repo / "include", helper, *runtime] if not p.exists()]
    if missing:
        raise RuntimeError("missing DagFlow files: " + ", ".join(map(str, missing)))

    definitions = {"DAGFLOW_BUILD_STATIC": "ON", "DAGFLOW_BUILD_PUBLIC_API_BENCH": "ON",
                   "DAGFLOW_ALLOCATOR": args.allocator, "DAGFLOW_PUBLIC_API_SOURCE": str(helper),
                   "DAGFLOW_ENABLE_NATIVE": "ON" if args.native else "OFF",
                   "CMAKE_CXX_FLAGS": args.cxxflags,
                   "DAGFLOW_USE_LLD": "ON" if "clang" in args.compiler else "OFF"}
    profile = "o3-lto" if args.lto else "o3"
    timing_commands = cmake_commands(repo, build_dir / "timing", args.compiler, profile,
                                    ["dagflow_public_api_bench"], definitions)
    diag_commands = cmake_commands(repo, build_dir / "diagnostics", args.compiler, profile,
                                  ["dagflow_public_api_bench"],
                                  dict(definitions, DAGFLOW_RUNTIME_DIAGNOSTICS="ON"))
    sources = [helper, *runtime]
    if not args.no_build:
        try:
            for command in timing_commands + diag_commands:
                run(command, cwd=repo)
            shutil.copy2(build_dir / "timing" / "dagflow-public-api-bench", timing)
            shutil.copy2(build_dir / "diagnostics" / "dagflow-public-api-bench", diagnostic)
        except subprocess.CalledProcessError as error:
            sys.stderr.write(error.stdout or "")
            sys.stderr.write(error.stderr or "")
            hint = ""
            if args.allocator == "mimalloc":
                hint = "\nHint: install mimalloc development files or use --allocator system."
            elif args.allocator == "tbbmalloc":
                hint = "\nHint: install oneTBB development files or use --allocator system."
            raise RuntimeError(f"benchmark build failed.{hint}") from error
    elif not timing.is_file() or not diagnostic.is_file():
        raise RuntimeError("--no-build requested but benchmark binaries are missing")

    compiler_version = run([args.compiler, "--version"]).stdout.splitlines()[0]
    manifest = {
        "schema": 1,
        "platform": platform.platform(),
        "python": sys.version,
        "compiler": compiler_version,
        "allocator": args.allocator,
        "workers": args.workers,
        "runs": args.runs,
        "warmup": args.warmup,
        "alloc_runs": args.alloc_runs,
        "alloc_warmup": args.alloc_warmup,
        "native": args.native,
        "lto": args.lto,
        "extra_cxxflags": args.cxxflags,
        "source_sha256": source_hash(sources + list((repo / "include" / "dagflow").rglob("*.hpp")) +
                                     [repo / "CMakeLists.txt"] + list((repo / "cmake").glob("*.cmake"))),
        "timing_binary_sha256": hashlib.sha256(timing.read_bytes()).hexdigest(),
        "diagnostic_binary_sha256": hashlib.sha256(diagnostic.read_bytes()).hexdigest(),
        "profile": profile,
        "timing_commands": timing_commands,
        "diagnostic_commands": diag_commands,
    }
    return timing, diagnostic, manifest


def invoke(binary: Path, benchmark: str, workers: int, runs_count: int, warmup: int,
           affinity: set[int] | None) -> dict:
    command = [
        binary,
        "--benchmark", benchmark,
        "--workers", str(workers),
        "--runs", str(runs_count),
        "--warmup", str(warmup),
    ]
    result = run(command, affinity=affinity)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise RuntimeError(f"expected one JSON line from {benchmark}, got {len(lines)}")
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


def format_bytes(value: float) -> str:
    for scale, suffix in ((1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")):
        if value >= scale:
            return f"{value / scale:.2f} {suffix}"
    return f"{value:.0f} B"


def make_markdown(rows: list[dict], manifest: dict) -> str:
    lines = [
        "# DagFlow public API benchmark",
        "",
        "The workload paths use public DagFlow APIs. Timing comes from a normal Release build; "
        "allocation counts come from a separate `DAGFLOW_RUNTIME_DIAGNOSTICS` build and are not "
        "timing measurements.",
        "",
        f"Workers: **{manifest['workers']}** · allocator: **{manifest['allocator']}** · "
        f"compiler: `{manifest['compiler']}` · LTO: **{manifest['lto']}** · "
        f"`-march=native`: **{manifest['native']}**",
        "",
        "| Benchmark | Runs | Mean | Min | Max | Throughput | Mean / unit | Runtime allocs / run | Requested bytes / run | Packet allocs / run |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {benchmark} | {runs} | {mean} | {min_} | {max_} | {rate} | {unit_time} | "
            "{allocs:.1f} | {bytes_} | {packets:.1f} |".format(
                benchmark=row["benchmark"],
                runs=row["runs"],
                mean=format_time(row["mean_s"]),
                min_=format_time(row["min_s"]),
                max_=format_time(row["max_s"]),
                rate=format_rate(row["throughput_per_s"], row["unit"]),
                unit_time=f"{row['mean_ns_per_unit']:.1f} ns/{row['unit']}",
                allocs=row["runtime_allocations_per_run"],
                bytes_=format_bytes(row["runtime_requested_bytes_per_run"]),
                packets=row["packet_allocations_per_run"],
            )
        )
    lines += [
        "",
        "`Runtime allocs` and `Requested bytes` count calls crossing DagFlow's runtime-memory boundary, "
        "not allocator-internal metadata/RSS. `Packet allocs` are the task-packet subset. "
        "Setup/reset of user data is outside the timed interval; graph construction is intentionally "
        "inside the chain/workflow interval because those rows benchmark the complete public API operation.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    try:
        args = parse_args()
        repo = args.repo.resolve()
        helper = (args.helper or (repo / "bench" / "public_api_bench.cpp")).resolve()
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=True)
        affinity = parse_cpu_list(args.cpus)
        if affinity is not None and not hasattr(os, "sched_setaffinity"):
            raise RuntimeError("--cpus requires Linux sched_setaffinity")

        timing, diagnostic, manifest = build(args, helper, repo, output)
        manifest["benchmarks"] = args.benchmarks
        manifest["cpus"] = sorted(affinity) if affinity is not None else None
        if affinity is None and hasattr(os, "sched_getaffinity"):
            manifest["allowed_cpus"] = sorted(os.sched_getaffinity(0))

        if args.build_only:
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(f"DagFlow benchmark binaries built successfully in {output / 'build'}")
            return 0

        rows: list[dict] = []
        raw: list[dict] = []
        for name in args.benchmarks:
            started = time.time()
            timing_row = invoke(timing, name, args.workers, args.runs, args.warmup, affinity)
            alloc_row = invoke(diagnostic, name, args.workers, args.alloc_runs, args.alloc_warmup, affinity)
            if timing_row["benchmark"] != alloc_row["benchmark"] or timing_row["logical_units"] != alloc_row["logical_units"]:
                raise RuntimeError(f"timing/diagnostic workload mismatch for {name}")
            merged = dict(timing_row)
            for key in (
                    "runtime_allocations_per_run",
                    "runtime_requested_bytes_per_run",
                    "packet_allocations_per_run",
                    "packet_bytes_per_run",
                    "cross_thread_frees_per_run",
            ):
                merged[key] = alloc_row[key]
            merged["diagnostic_runs"] = alloc_row["runs"]
            merged["wall_seconds"] = time.time() - started
            rows.append(merged)
            raw.append({"name": name, "timing": timing_row, "diagnostic": alloc_row})

        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
        (output / "raw.json").write_text(json.dumps(raw, indent=2) + "\n")

        fieldnames = sorted({key for row in rows for key in row if key != "samples_s"})
        with (output / "results.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows({k: v for k, v in row.items() if k != "samples_s"} for row in rows)

        report = make_markdown(rows, manifest)
        (output / "report.md").write_text(report + "\n")
        print(report)
        print(f"\nRaw results: {output / 'results.json'}", file=sys.stderr)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"public API benchmark: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
