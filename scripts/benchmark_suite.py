#!/usr/bin/env python3
"""Sequential DagFlow benchmark matrix, builds, PGO training and perf sidecars."""
import argparse
import csv
import datetime as dt
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import sys
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from dagflow_harness_bridge import CommandRunner

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ("release", "lto", "pgo", "lto-pgo")


def integers(value):
    try:
        result = [int(item) for item in value.split(",")]
        if not result or any(item < 0 for item in result):
            raise ValueError()
        return list(dict.fromkeys(result))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated nonnegative integers") from error


def parse():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="use an existing suite; skip builds")
    parser.add_argument("--profiles", default="release", help="release,lto,pgo,lto-pgo (Clang builds)")
    parser.add_argument("--allocator", choices=("system", "mimalloc", "tbbmalloc"), default="mimalloc")
    parser.add_argument("--compiler", default="clang++")
    parser.add_argument("--shards", type=int, default=0, help="shard count; 0 means workers")
    parser.add_argument("--workers", type=integers, default=[1, 2, 4])
    parser.add_argument("--work-ns", type=integers, default=[0, 100, 1000, 10000])
    parser.add_argument("--scenarios", default="all", help="comma-separated names, or all")
    parser.add_argument("--tasks", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=15)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--sample-stride", type=int, default=16)
    parser.add_argument("--idle-us", type=int, default=2000)
    parser.add_argument("--modes", default="throughput,latency")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=180, help="seconds per benchmark invocation")
    parser.add_argument("--perf", action="store_true", help="separate Linux perf stat runs; errors retained")
    parser.add_argument("--perf-events", default="cycles,instructions,cache-misses,context-switches")
    parser.add_argument("--output", type=Path, default=ROOT / "out" / "benchmarks" / dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f"))
    args = parser.parse_args()
    args.profiles = list(dict.fromkeys(args.profiles.split(",")))
    args.modes = list(dict.fromkeys(args.modes.split(",")))
    if any(p not in PROFILES for p in args.profiles):
        parser.error("unknown build profile")
    if any(m not in ("throughput", "latency") for m in args.modes):
        parser.error("modes must be throughput and/or latency")
    if args.binary and args.profiles != ["release"]:
        parser.error("--profiles applies only when building, not with --binary")
    if (args.shards < 0 or min(args.workers) < 1 or args.tasks < 1 or args.repeats < 1 or
            args.warmup < 0 or args.sample_stride < 1 or args.jobs < 1 or
            args.idle_us < 0 or args.timeout <= 0):
        parser.error("invalid worker/count/interval option")
    return args


def capture(command, **kwargs):
    return subprocess.check_output(command, text=True, **kwargs).strip()


def source_manifest():
    paths = [ROOT / "CMakeLists.txt", ROOT / "CMakePresets.json"]
    for directory in ("include", "src", "bench", "scripts", "cmake"):
        paths.extend(p for p in (ROOT / directory).rglob("*") if p.suffix in (".hpp", ".cpp", ".py", ".sh", ".cmake"))
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


class Runner:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        (self.output / "logs").mkdir()
        self.processes = CommandRunner(self.output, args.timeout)

    def command(self, command, label, timeout=None):
        print(f"[{label}]", flush=True)
        self.processes.timeout = timeout if timeout is not None else 600
        _, output = self.processes.command(list(map(str, command)), label, cwd=ROOT)
        return output

    def configure_build(self, name, lto=False, pgo="none", profile=None):
        directory = self.output / "build" / name
        build_profile = ("lto-" if lto else "") + "pgo-" + ("generate" if pgo == "generate" else "use") if pgo != "none" else ("lto" if lto else "release")
        command = ["cmake", "-S", ROOT, "-B", directory, "-G", "Ninja",
                   f"-DCMAKE_CXX_COMPILER={self.args.compiler}", f"-DDAGFLOW_PROFILE={build_profile}",
                   "-DDAGFLOW_BUILD_SHARED=OFF", "-DDAGFLOW_BUILD_STATIC=ON", "-DDAGFLOW_BUILD_EXAMPLES=OFF",
                   "-DDAGFLOW_BUILD_BENCH=OFF", "-DDAGFLOW_BUILD_RUNTIME_BENCH=OFF",
                   "-DDAGFLOW_BUILD_RUNTIME_SUITE=ON", "-DDAGFLOW_BUILD_TESTS=OFF",
                   "-DDAGFLOW_INSTALL=OFF", f"-DDAGFLOW_ALLOCATOR={self.args.allocator}",
                   "-DDAGFLOW_USE_LLD=ON", f"-DDAGFLOW_PGO_DIR={directory / 'raw'}",
                   f"-DDAGFLOW_PGO_MERGED_PROFILE={directory / 'suite.profdata'}"]
        if profile:
            command.append(f"-DDAGFLOW_PGO_PROFILE={profile}")
        self.command(command, name + "-configure")
        self.command(["cmake", "--build", directory, "--target", "dagflow_runtime_suite",
                      "-j", self.args.jobs], name + "-build")
        return directory / "dagflow-runtime-suite"

    def build(self, name):
        lto = name in ("lto", "lto-pgo")
        if name not in ("pgo", "lto-pgo"):
            return self.configure_build(name, lto=lto)
        trainer = self.configure_build(name + "-train", lto=lto, pgo="generate")
        # CMake owns the explicit training/merge target; samples never enter results.
        self.command(["cmake", "--build", trainer.parent, "--target", "dagflow_pgo_merge"],
                     name + "-training-merge", self.args.timeout)
        profile = trainer.parent / "suite.profdata"
        return self.configure_build(name, lto=lto, pgo="use", profile=profile)

    def run(self):
        a = self.args
        manifest = {"schema": 1, "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "platform": platform.platform(), "python": sys.version,
                    "cpu_count": os.cpu_count(), "source_sha256": source_manifest(),
                    "options": {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()}}
        if hasattr(os, "sched_getaffinity"):
            manifest["allowed_cpus"] = sorted(os.sched_getaffinity(0))
        if Path("/proc/cpuinfo").exists():
            (self.output / "cpuinfo.txt").write_text(Path("/proc/cpuinfo").read_text())
        for key, command in (("git_head", ["git", "rev-parse", "HEAD"]),
                             ("git_status", ["git", "status", "--short"])):
            try:
                manifest[key] = capture(command, cwd=ROOT)
            except subprocess.CalledProcessError:
                manifest[key] = None
        if a.binary:
            binaries = {"existing": a.binary.resolve()}
            if not a.binary.is_file():
                raise RuntimeError("suite binary does not exist")
        else:
            compiler = capture([a.compiler, "--version"])
            if "clang" not in compiler.lower():
                raise RuntimeError("the build matrix currently requires Clang; use --binary for other compilers")
            manifest["compiler"] = compiler
            # One release calibration is reused across every worker/profile.
            binaries = {"release": self.build("release")}
            for name in a.profiles:
                if name != "release":
                    binaries[name] = self.build(name)
        reference = next(iter(binaries.values()))
        calibration = json.loads(self.command([reference, "--calibrate"], "calibration", a.timeout))
        manifest["calibration"] = calibration
        manifest["binaries_sha256"] = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
        available = self.command([reference, "--list"], "scenarios", a.timeout).splitlines()
        scenarios = available if a.scenarios == "all" else list(dict.fromkeys(a.scenarios.split(",")))
        if any(s not in available for s in scenarios):
            raise RuntimeError("unknown scenario in --scenarios")
        (self.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        profiles = ["existing"] if a.binary else a.profiles
        # Shuffle deterministically to distribute thermal/order bias; never run
        # timed benchmarks concurrently. Original invocation order is retained.
        points = list(itertools.product(profiles, a.workers, a.work_ns, scenarios, a.modes))
        random.Random(1729).shuffle(points)
        rows, checksums = [], {}
        perf_status = []
        with (self.output / "results.jsonl").open("w") as output:
            for index, (profile, workers, ns, scenario, mode) in enumerate(points):
                iterations = max(1, round(ns / calibration["ns_per_iteration"])) if ns else 0
                command = [binaries[profile], "--scenario", scenario, "--workers", workers,
                           "--shards", a.shards, "--tasks", a.tasks, "--repeats", a.repeats, "--warmup", a.warmup,
                           "--iterations", iterations, "--work-ns", ns, "--idle-us", a.idle_us,
                           "--sample-stride", a.sample_stride]
                if mode == "latency":
                    command.append("--latency")
                label = f"run-{index:05d}-{profile}-{scenario}-{workers}-{ns}-{mode}"
                lines = self.command(command, label, a.timeout).splitlines()
                if len(lines) != 1:
                    raise RuntimeError("expected one JSON result per scenario invocation")
                row = json.loads(lines[0])
                row.update(profile=profile, mode=mode, workers=workers, work_ns=ns,
                           tasks=a.tasks, iterations=iterations, sequence=index)
                if row["status"] == "ok":
                    key = (scenario, iterations)
                    if key in checksums and checksums[key] != row["checksum"]:
                        raise RuntimeError("checksum differs across builds/workers/modes")
                    checksums[key] = row["checksum"]
                rows.append(row)
                output.write(json.dumps(row) + "\n")
                output.flush()
                if a.perf and mode == "throughput" and row["status"] == "ok":
                    stat = self.output / "logs" / (label + ".perf.csv")
                    try:
                        self.command(["perf", "stat", "-x", ";", "-o", stat, "-e", a.perf_events,
                                      "--", *command], label + "-perf", a.timeout)
                        content = stat.read_text()
                        status = "unavailable" if "<not supported>" in content or "<not counted>" in content else "ok"
                        perf_status.append({"sequence": index, "status": status, "file": str(stat)})
                    except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                        perf_status.append({"sequence": index, "status": "failed", "error": str(error)})
        fields = sorted({key for row in rows for key in row if key != "run_us"})
        with (self.output / "results.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        (self.output / "perf-status.json").write_text(json.dumps(perf_status, indent=2) + "\n")
        print(f"Wrote {len(rows)} rows to {self.output}")


def main():
    try:
        Runner(parse()).run()
    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"benchmark suite: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
