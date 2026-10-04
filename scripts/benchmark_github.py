#!/usr/bin/env python3
"""Compare the local GitHub snapshot and current runtime with one suite source."""
import argparse
from collections import Counter
import csv
import datetime as dt
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
FRESH_GRAPHS = {"fanout_fanin", "deep_dag", "graph_tokens_serial", "graph_tokens_parallel"}


def numbers(value):
    return list(dict.fromkeys(int(x) for x in value.split(",")))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def report(output, rows):
    lines = ["# GitHub snapshot vs current DagFlow", "",
             "Same suite, Clang -O3 -DNDEBUG, system allocator, sequential paired runs.",
             "One calibration supplies identical payload iterations to both runtimes.",
             "Fresh graphs are built outside timing for fanout, deep DAG and token cases.",
             "graph_reuse deliberately retains the same graph; dag_build_run includes construction.",
             "Current includes the park/wake, empty-steal and scope/completion changes.", "",
             "Ratio = GitHub median / current median; >1 means current is faster.",
             "Each median describes one invocation; raw repeats are in results.jsonl.", "",
             "## Status", "", "| Backend | Status | Invocations |", "| --- | --- | ---: |"]
    for (backend, status), count in sorted(Counter((r["backend"], r["status"]) for r in rows).items()):
        lines.append(f"| {backend} | {status} | {count} |")
    failed = Counter((r["backend"], r["scenario"], r.get("reason", ""))
                     for r in rows if r["status"] not in ("ok", "skipped"))
    if failed:
        lines += ["", "## Failures", ""]
        for (backend, scenario, reason), count in failed.items():
            lines.append(f"- {backend} / {scenario}: {count} invocations; {reason.strip()}")
    by_key = {(r["profile"], r["workers"], r["work_ns"], r["scenario"], r["mode"], r["backend"]): r
              for r in rows}
    comparisons = []
    for key, old in by_key.items():
        if key[-1] != "github" or old["status"] != "ok":
            continue
        new = by_key.get((*key[:-1], "current"))
        if not new or new["status"] != "ok":
            continue
        comparisons.append(dict(profile=key[0], workers=key[1], work_ns=key[2], scenario=key[3],
                                mode=key[4], github_us=old["run_p50_us"], current_us=new["run_p50_us"],
                                github_over_current=old["run_p50_us"] / new["run_p50_us"],
                                github_latency_p99_us=old["latency_p99_us"],
                                current_latency_p99_us=new["latency_p99_us"]))
    comparisons.sort(key=lambda r: (r["profile"], r["workers"], r["work_ns"], r["mode"], r["scenario"]))
    if comparisons:
        with (output / "comparison.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(comparisons[0]))
            writer.writeheader()
            writer.writerows(comparisons)
    for profile, workers, work_ns in sorted({(r["profile"], r["workers"], r["work_ns"]) for r in comparisons}):
        lines += ["", f"## {profile}, {workers} workers, nominal payload {work_ns} ns", "",
                  "| Scenario | GitHub µs | Current µs | Ratio |", "| --- | ---: | ---: | ---: |"]
        for r in comparisons:
            if (r["profile"], r["workers"], r["work_ns"], r["mode"]) == (profile, workers, work_ns, "throughput"):
                lines.append(f'| {r["scenario"]} | {r["github_us"]:.2f} | {r["current_us"]:.2f} | {r["github_over_current"]:.3f} |')
    (output / "report.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy", type=Path, required=True,
                        help="external snapshot directory containing include/ and src/")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=numbers, default=[1, 2, 4, 8])
    parser.add_argument("--work-ns", type=numbers, default=[0, 1000])
    parser.add_argument("--profiles", default="release,lto")
    parser.add_argument("--scenarios", default="all")
    parser.add_argument("--modes", default="throughput,latency")
    parser.add_argument("--tasks", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--compiler", default="clang++")
    a = parser.parse_args()
    if not (a.legacy / "src" / "thread_pool.cpp").is_file():
        parser.error("--legacy must contain src/thread_pool.cpp")
    profiles, modes = a.profiles.split(","), a.modes.split(",")
    if (not set(profiles) <= {"release", "lto"} or not set(modes) <= {"throughput", "latency"}
            or min(a.workers) < 1 or min(a.work_ns) < 0 or a.tasks < 1 or a.repeats < 1
            or a.warmup < 0 or a.timeout <= 0):
        parser.error("invalid matrix/count/timeout")
    output = a.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "logs").mkdir()
    (output / "bin").mkdir()
    commands = []

    def run(command, label, timeout=300):
        command = list(map(str, command))
        entry = {"label": label, "argv": command, "started_unix": time.time()}
        commands.append(entry)
        try:
            p = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
            entry["returncode"] = p.returncode
            stdout, stderr = p.stdout, p.stderr
        except subprocess.TimeoutExpired as error:
            entry["timeout"] = timeout
            stdout, stderr = error.stdout or b"", error.stderr or b""
            p = None
        for suffix, content in (("stdout", stdout), ("stderr", stderr)):
            if isinstance(content, bytes):
                content = content.decode(errors="replace")
            (output / "logs" / f"{label}.{suffix}").write_text(content)
        entry["elapsed_seconds"] = time.time() - entry["started_unix"]
        (output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
        return p

    manifest = {"started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "options": {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()},
                "compiler": subprocess.check_output([a.compiler, "--version"], text=True),
                "platform": platform.platform(), "allowed_cpus": sorted(os.sched_getaffinity(0)),
                "initial_loadavg": list(os.getloadavg()), "sources": {}, "binaries": {},
                "fresh_graph_scenarios": sorted(FRESH_GRAPHS)}
    (output / "cpuinfo.txt").write_text(Path("/proc/cpuinfo").read_text())
    # Copy source inputs only, without reading README or modifying either runtime.
    for backend, origin in (("github", a.legacy.resolve()), ("current", ROOT)):
        dest = output / "source" / backend
        paths = sorted(p for directory in ("src", "include") for p in (origin / directory).rglob("*")
                       if p.suffix in (".cpp", ".hpp"))
        manifest["sources"][backend] = {}
        for p in paths:
            relative = p.relative_to(origin)
            target = dest / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(p, target)
            manifest["sources"][backend][str(relative)] = digest(target)
        for name in ("runtime_suite.cpp", "runtime_suite_backend.hpp"):
            target = dest / "bench" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / "bench" / name, target)
            manifest["sources"][backend][f"bench/{name}"] = digest(target)
    shutil.copyfile(Path(__file__), output / "runner.py")
    manifest["runner_sha256"] = digest(output / "runner.py")

    def save_manifest():
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    save_manifest()
    # CMake owns translation units, dependencies and profile flags for both runtimes.
    current_source = output / "source" / "current"
    shutil.copyfile(ROOT / "CMakeLists.txt", current_source / "CMakeLists.txt")
    shutil.copytree(ROOT / "cmake", current_source / "cmake")
    for path in [current_source / "CMakeLists.txt", *sorted((current_source / "cmake").rglob("*"))]:
        if path.is_file():
            manifest["sources"]["current"][str(path.relative_to(current_source))] = digest(path)
    binaries = {}
    for profile in profiles:
        build = output / "build" / profile
        configure = ["cmake", "-S", current_source, "-B", build, "-G", "Ninja",
                     f"-DCMAKE_CXX_COMPILER={a.compiler}", f"-DDAGFLOW_PROFILE={profile}",
                     "-DDAGFLOW_ALLOCATOR=system", "-DDAGFLOW_USE_LLD=ON",
                     "-DDAGFLOW_BUILD_STATIC=ON", "-DDAGFLOW_BUILD_SHARED=OFF", "-DDAGFLOW_BUILD_TESTS=OFF",
                     "-DDAGFLOW_BUILD_EXAMPLES=OFF", "-DDAGFLOW_BUILD_BENCH=OFF", "-DDAGFLOW_INSTALL=OFF",
                     "-DDAGFLOW_BUILD_RUNTIME_BENCH=OFF", "-DDAGFLOW_BUILD_RUNTIME_SUITE=ON",
                     "-DDAGFLOW_BUILD_GITHUB_BENCH=ON",
                     f"-DDAGFLOW_GITHUB_SOURCE_DIR={output / 'source' / 'github'}"]
        for label, command in ((f"configure-{profile}", configure),
                               (f"build-{profile}", ["cmake", "--build", build, "--target",
                                                      "dagflow_runtime_suite", "dagflow_github_suite", "-j4"])):
            print(label, flush=True)
            result = run(command, label)
            if result is None or result.returncode:
                raise RuntimeError(f"{label} failed; see {output / 'logs'}")
        for backend, name in (("github", "dagflow-github-suite"), ("current", "dagflow-runtime-suite")):
            binary = output / "bin" / f"{backend}-{profile}"
            shutil.copy2(build / name, binary)
            binaries[profile, backend] = binary
            manifest["binaries"][binary.name] = digest(binary)
        save_manifest()
    reference = binaries[profiles[0], "current"]
    calibration = run([reference, "--calibrate"], "calibration")
    manifest["calibration"] = json.loads(calibration.stdout)
    save_manifest()
    available = run([reference, "--list"], "scenarios").stdout.splitlines()
    scenarios = available if a.scenarios == "all" else a.scenarios.split(",")
    if not set(scenarios) <= set(available):
        parser.error("unknown scenario")
    points = list(itertools.product(profiles, a.workers, a.work_ns, scenarios, modes))
    rng = random.Random(1729)
    rng.shuffle(points)
    rows = []
    with (output / "results.jsonl").open("w") as stream:
        for index, (profile, workers, ns, scenario, mode) in enumerate(points):
            iterations = max(1, round(ns / manifest["calibration"]["ns_per_iteration"])) if ns else 0
            backends = ["github", "current"]
            rng.shuffle(backends)
            pair = []
            for backend in backends:
                command = [binaries[profile, backend], "--scenario", scenario, "--workers", workers,
                           "--tasks", a.tasks, "--iterations", iterations, "--work-ns", ns,
                           "--repeats", a.repeats, "--warmup", a.warmup]
                if scenario in FRESH_GRAPHS:
                    command += ["--fresh-graph"]
                if mode == "latency":
                    command += ["--latency"]
                label = f"run-{index:04d}-{backend}-{profile}-{workers}-{ns}-{scenario}-{mode}"
                p = run(command, label, a.timeout)
                if p is None:
                    row = dict(status="timeout", reason=f"exceeded {a.timeout}s")
                elif p.returncode:
                    row = dict(status="failed", reason=p.stderr.strip(), returncode=p.returncode)
                else:
                    try:
                        row = json.loads(p.stdout)
                    except ValueError:
                        row = dict(status="failed", reason="invalid JSON output")
                row.update(backend=backend, profile=profile, workers=workers, work_ns=ns,
                           scenario=scenario, mode=mode, sequence=index, iterations=iterations, log=label)
                pair.append(row)
                if row["status"] not in ("ok", "skipped"):
                    print(label, row["status"], row.get("reason"), flush=True)
            if all(r["status"] == "ok" for r in pair) and pair[0]["checksum"] != pair[1]["checksum"]:
                for row in pair:
                    row.update(status="failed", reason="checksum mismatch between runtimes")
            for row in pair:
                rows.append(row)
                stream.write(json.dumps(row) + "\n")
            stream.flush()
            if index % 16 == 0 or index + 1 == len(points):
                print(f"{index + 1}/{len(points)} pairs complete", flush=True)
                report(output, rows)
    manifest["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest["final_loadavg"] = list(os.getloadavg())
    save_manifest()
    report(output, rows)
    print(output / "report.md", flush=True)


if __name__ == "__main__":
    main()
