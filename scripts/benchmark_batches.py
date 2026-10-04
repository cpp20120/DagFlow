#!/usr/bin/env python3
"""Paired scalar/batch experiment. Linux taskset; raw samples are retained."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--repeats", type=int, default=101)
    parser.add_argument("--warmup", type=int, default=10)
    args = parser.parse_args()
    if args.trials < 1 or args.repeats < 1 or args.warmup < 0:
        parser.error("invalid repetition count")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {v: p.resolve() for v, p in (("before", args.before), ("after", args.after))}
    cores = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        key = tuple((topology / f).read_text().strip() for f in ("physical_package_id", "core_id"))
        cores.setdefault(key, cpu)
    cpus = list(cores.values())
    # Scalar control in both binaries distinguishes code-layout/build effects
    # from invoking the new API. batch=1 exposes staging/API overhead.
    variants = [("before", 0), ("after", 0), *[("after", n) for n in (1, 4, 16, 64)]]
    points = [(w, "external_detached", 4096, it, False) for w in (1, 4) for it in (0, 400)]
    points += [(w, s, 1, 0, True) for w in (1, 4) for s in ("external_detached", "idle_burst")]
    rng = random.Random(947)
    rng.shuffle(points)
    rows = []
    manifest = {"binaries": {v: {"path": str(p), "sha256": digest(p)} for v, p in binaries.items()},
                "trials": args.trials, "repeats": args.repeats, "warmup": args.warmup,
                "physical_core_representatives": cpus,
                "placement": "process restricted to workers+1 distinct cores; threads not individually pinned",
                "frequency_locked": False}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with (args.output / "paired.jsonl").open("w") as raw, (args.output / "commands.jsonl").open("w") as command_log:
        for trial in range(args.trials):
            for workers, scenario, tasks, iterations, latency in points:
                order = variants.copy()
                rng.shuffle(order)
                mask = ",".join(map(str, cpus[:min(workers + 1, len(cpus))]))
                for version, batch in order:
                    command = ["taskset", "-c", mask, str(binaries[version]), "--workers", str(workers),
                               "--scenario", scenario, "--tasks", str(tasks), "--iterations", str(iterations),
                               "--warmup", str(args.warmup), "--repeats", str(args.repeats)]
                    if version == "after":
                        command += ["--submit-batch", str(batch)]
                    if latency:
                        command += ["--latency", "--sample-stride", "1"]
                    command_log.write(json.dumps(command) + "\n")
                    command_log.flush()
                    row = json.loads(subprocess.check_output(command, text=True, timeout=120))
                    assert row["status"] == "ok"
                    row.update(version=version, submit_batch=batch, trial=trial, cpus=mask)
                    rows.append(row)
                    raw.write(json.dumps(row) + "\n")
                    raw.flush()
            print(f"Trial {trial + 1}/{args.trials}", flush=True)
    summary = []
    for workers, scenario, tasks, iterations, latency in sorted(points):
        group = [r for r in rows if (r["workers"], r["scenario"], r["tasks"], r["iterations"]) ==
                 (workers, scenario, tasks, iterations)]
        assert len({r["checksum"] for r in group}) == 1
        def selected(version, batch):
            return [r for r in group if r["version"] == version and r["submit_batch"] == batch]
        baseline = selected("before", 0)
        baseline_us = statistics.median(r["run_p50_us"] for r in baseline)
        for version, batch in variants:
            results = selected(version, batch)
            paired = [r["run_p50_us"] / next(b["run_p50_us"] for b in baseline if b["trial"] == r["trial"])
                      for r in results]
            median_us = statistics.median(r["run_p50_us"] for r in results)
            item = dict(workers=workers, scenario=scenario, tasks=tasks, iterations=iterations,
                        version=version, batch=batch, median_us=median_us,
                        ratio=median_us / baseline_us, paired_ratios=paired)
            if latency:
                item.update({k: statistics.median(r[k] for r in results)
                             for k in ("latency_p50_us", "latency_p99_us", "latency_p999_us")})
            summary.append(item)
            print(json.dumps(item), flush=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
