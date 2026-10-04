from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import random
import shutil
import sys
import time
from typing import Any

from .core import (
    CommandRunner,
    digest,
    host_manifest,
    load_cases,
    parse_cpu_list,
    physical_cpus,
    save_json,
    summarize_runs,
)


def _key_value(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected KEY=VALUE")
    key, raw = value.split("=", 1)
    if not key:
        raise argparse.ArgumentTypeError("empty key")
    return key, raw


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reusable process benchmark/stress harness")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run a binary over one case or a JSON case matrix")
    run.add_argument("--name", required=True)
    run.add_argument("--binary", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--cases", type=Path)
    run.add_argument("--arg", action="append", default=[])
    run.add_argument("--env", action="append", default=[], type=_key_value)
    run.add_argument("--meta", action="append", default=[], type=_key_value)
    run.add_argument("--metric", action="append", default=[])
    run.add_argument("--invariant", action="append", default=[])
    run.add_argument("--perf-event", action="append", default=[])
    run.add_argument("--parser", choices=("json-line", "json", "none"), default="json-line")
    run.add_argument("--rounds", type=int, default=1, help="measured process launches per case")
    run.add_argument("--warmup-runs", type=int, default=0, help="warmup process launches per case")
    run.add_argument("--timeout", type=float, default=60.0)
    run.add_argument("--working-directory", type=Path)
    run.add_argument("--affinity", choices=("inherit", "physical", "none"), default="inherit")
    run.add_argument("--cpu-count", type=int, default=0)
    run.add_argument("--cpus", default="")
    run.add_argument("--seed", type=lambda x: int(x, 0), default=0xC0FFEE)
    run.add_argument("--randomize", action="store_true")
    run.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def _select_cpus(case, args, physical: list[int]) -> tuple[int, ...]:
    if case.cpus:
        selected = case.cpus
    elif case.cpu_count:
        selected = tuple(physical[: case.cpu_count])
    elif args.cpus:
        selected = parse_cpu_list(args.cpus)
    elif args.affinity == "physical" and args.cpu_count:
        selected = tuple(physical[: args.cpu_count])
    elif args.affinity == "none":
        selected = ()
    elif hasattr(os, "sched_getaffinity"):
        selected = tuple(sorted(os.sched_getaffinity(0)))
    else:
        selected = ()
    if args.affinity == "physical" and not selected:
        selected = tuple(physical)
    if hasattr(os, "sched_getaffinity") and selected:
        allowed = set(os.sched_getaffinity(0))
        if not set(selected) <= allowed:
            raise ValueError(f"requested CPUs {selected} are not within allowed affinity {sorted(allowed)}")
    return selected


def run_command(args: argparse.Namespace) -> int:
    if args.rounds < 1 or args.warmup_runs < 0 or args.timeout <= 0 or args.cpu_count < 0:
        raise SystemExit("rounds must be positive; warmup/cpu-count nonnegative; timeout positive")
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"binary does not exist: {binary}")
    output = args.output.resolve()
    if output.exists():
        if not args.overwrite:
            raise SystemExit(f"output directory already exists: {output}; pass --overwrite to replace it")
        shutil.rmtree(output)
    output.mkdir(parents=True)
    cases = load_cases(args.cases.resolve() if args.cases else None, args.arg)
    physical, _ = physical_cpus()
    metadata = dict(args.meta)
    manifest: dict[str, Any] = dict(
        schema=1,
        name=args.name,
        created_unix=time.time(),
        binary=dict(path=str(binary), sha256=digest(binary), size=binary.stat().st_size),
        cases_file=str(args.cases.resolve()) if args.cases else None,
        cases=[dict(name=case.name, args=list(case.args), metadata=case.metadata) for case in cases],
        execution=dict(
            rounds=args.rounds,
            warmup_runs=args.warmup_runs,
            timeout=args.timeout,
            parser=args.parser,
            affinity=args.affinity,
            cpu_count=args.cpu_count,
            cpus=args.cpus,
            seed=args.seed,
            randomize=args.randomize,
            perf_events=args.perf_event,
        ),
        metrics=args.metric,
        invariants=args.invariant,
        metadata=metadata,
        host=host_manifest(),
    )
    if args.cases:
        manifest["cases_sha256"] = digest(args.cases.resolve())
    save_json(output / "manifest.json", manifest)
    runner = CommandRunner(output, args.timeout, dict(args.env))
    rows: list[dict[str, Any]] = []
    rng = random.Random(args.seed)

    jobs: list[tuple[bool, int, Any]] = []
    for warmup in range(args.warmup_runs):
        for case in cases:
            jobs.append((True, warmup, case))
    measured: list[tuple[bool, int, Any]] = []
    for round_index in range(args.rounds):
        round_cases = list(cases)
        if args.randomize:
            rng.shuffle(round_cases)
        for case in round_cases:
            measured.append((False, round_index, case))
    jobs.extend(measured)

    with (output / "runs.jsonl").open("w") as stream:
        counters: dict[tuple[bool, str], int] = {}
        for warmup, round_index, case in jobs:
            key = (warmup, case.name)
            sequence = counters.get(key, 0)
            counters[key] = sequence + 1
            phase = "warmup" if warmup else "run"
            label = f"{phase}-{sequence:03d}-{case.name}"
            cpus = _select_cpus(case, args, physical)
            cwd = Path(case.cwd).resolve() if case.cwd else (
                args.working_directory.resolve() if args.working_directory else None
            )
            entry = runner.run(
                [str(binary), *case.args],
                label,
                cwd=cwd,
                env=case.env,
                cpus=cpus,
                parser=args.parser,
                perf_events=tuple(args.perf_event),
            )
            row = dict(entry)
            row.update(
                case=case.name,
                case_metadata=case.metadata,
                warmup=warmup,
                round=round_index,
            )
            rows.append(row)
            stream.write(json.dumps(row, sort_keys=True) + "\n")
            stream.flush()

    summary = summarize_runs(rows, args.metric, args.invariant)
    save_json(output / "summary.json", summary)
    manifest["completed_unix"] = time.time()
    manifest["summary"] = str(output / "summary.json")
    save_json(output / "manifest.json", manifest)
    print(output / "summary.json")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        return run_command(args)
    raise AssertionError(args.command)
