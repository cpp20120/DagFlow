from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import signal
import statistics
import subprocess
import sys
import time
from typing import Any, Iterable


@dataclass(frozen=True)
class Case:
    name: str
    args: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    cpu_count: int | None = None
    cpus: tuple[int, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def parse_cpu_list(text: str | None) -> tuple[int, ...]:
    if not text:
        return ()
    cpus: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo_text, hi_text = part.split("-", 1)
            lo, hi = int(lo_text), int(hi_text)
            if hi < lo:
                raise ValueError(f"invalid CPU range: {part}")
            cpus.update(range(lo, hi + 1))
        else:
            cpus.add(int(part))
    if any(cpu < 0 for cpu in cpus):
        raise ValueError("CPU indices must be nonnegative")
    return tuple(sorted(cpus))


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def physical_cpus() -> tuple[list[int], list[dict[str, Any]]]:
    if not hasattr(os, "sched_getaffinity"):
        count = os.cpu_count() or 1
        return list(range(count)), [dict(cpu=i) for i in range(count)]
    allowed = sorted(os.sched_getaffinity(0))
    representatives: list[int] = []
    topology: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | int | None]] = set()
    for cpu in allowed:
        base = Path(f"/sys/devices/system/cpu/cpu{cpu}")
        package_id = _read_text(base / "topology/physical_package_id")
        core_id = _read_text(base / "topology/core_id")
        key = (package_id, core_id if core_id is not None else cpu)
        topology.append(
            dict(
                cpu=cpu,
                package=package_id,
                core=core_id,
                siblings=_read_text(base / "topology/thread_siblings_list"),
                governor=_read_text(base / "cpufreq/scaling_governor"),
                frequency_khz=_read_text(base / "cpufreq/scaling_cur_freq"),
            )
        )
        if key not in seen:
            seen.add(key)
            representatives.append(cpu)
    return representatives, topology


def host_manifest() -> dict[str, Any]:
    representatives, topology = physical_cpus()
    affinity = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    uname = platform.uname()
    return dict(
        platform=sys.platform,
        python=sys.version.split()[0],
        machine=platform.machine(),
        processor=platform.processor(),
        uname=dict(system=uname.system, release=uname.release, version=uname.version, node=uname.node),
        logical_cpus=os.cpu_count(),
        allowed_cpus=affinity,
        physical_core_representatives=representatives,
        cpu_topology=topology,
        loadavg=list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
    )


def parse_perf_stat(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return result
    for line in path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        cells = line.split(";")
        if len(cells) < 3:
            continue
        raw_value, unit, event = (cell.strip() for cell in cells[:3])
        if not event:
            continue
        try:
            value: float | None = float(raw_value.replace(",", ""))
        except ValueError:
            value = None
        running: float | None = None
        if len(cells) > 4:
            try:
                running = float(cells[4].strip().rstrip("%"))
            except ValueError:
                pass
        result[event] = dict(
            value=value,
            unit=unit,
            running_percent=running,
            status="ok" if value is not None else raw_value,
        )
    return result


def _parse_result(stdout: str, parser: str) -> dict[str, Any] | None:
    if parser == "none":
        return None
    if parser == "json":
        value = json.loads(stdout)
        if not isinstance(value, dict):
            raise ValueError("JSON result must be an object")
        return value
    if parser == "json-line":
        values: list[dict[str, Any]] = []
        for line in stdout.splitlines():
            stripped = line.strip()
            if not stripped.startswith("{"):
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                values.append(value)
        if len(values) != 1:
            raise ValueError(f"expected exactly one JSON object line, found {len(values)}")
        return values[0]
    raise ValueError(f"unknown parser: {parser}")


def lookup_field(value: dict[str, Any], path: str) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(path)
        current = current[part]
    return current


def load_cases(path: Path | None, args: Iterable[str]) -> list[Case]:
    if path is None:
        return [Case("default", tuple(map(str, args)))]
    raw = json.loads(path.read_text())
    if isinstance(raw, dict):
        raw = raw.get("cases")
    if not isinstance(raw, list) or not raw:
        raise ValueError("cases file must contain a nonempty JSON array or {\"cases\": [...]} object")
    cases: list[Case] = []
    names: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"case {index} must be an object")
        name = str(item.get("name", f"case-{index}"))
        if not name or name in names:
            raise ValueError(f"duplicate/empty case name: {name!r}")
        names.add(name)
        argv = item.get("args", [])
        env = item.get("env", {})
        cpus = item.get("cpus", [])
        metadata = item.get("metadata", {})
        if not isinstance(argv, list) or not all(isinstance(v, (str, int, float)) for v in argv):
            raise ValueError(f"case {name}: args must be a scalar array")
        if not isinstance(env, dict) or not all(isinstance(k, str) for k in env):
            raise ValueError(f"case {name}: env must be an object")
        if not isinstance(cpus, list) or not all(isinstance(v, int) and v >= 0 for v in cpus):
            raise ValueError(f"case {name}: cpus must be nonnegative integers")
        if not isinstance(metadata, dict):
            raise ValueError(f"case {name}: metadata must be an object")
        cpu_count = item.get("cpu_count")
        if cpu_count is not None and (not isinstance(cpu_count, int) or cpu_count < 1):
            raise ValueError(f"case {name}: cpu_count must be positive")
        cases.append(
            Case(
                name=name,
                args=tuple(map(str, argv)),
                env={str(k): str(v) for k, v in env.items()},
                cwd=str(item["cwd"]) if item.get("cwd") is not None else None,
                cpu_count=cpu_count,
                cpus=tuple(cpus),
                metadata=metadata,
            )
        )
    return cases


class CommandRunner:
    def __init__(self, output: Path, timeout: float, base_env: dict[str, str] | None = None):
        self.output = output
        self.timeout = timeout
        self.logs = output / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.commands: list[dict[str, Any]] = []
        self.base_env = dict(os.environ)
        self.base_env.setdefault("LC_ALL", "C")
        self.base_env.setdefault("DEBUGINFOD_URLS", "")
        if base_env:
            self.base_env.update(base_env)

    def run(
        self,
        argv: list[str],
        label: str,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        cpus: tuple[int, ...] = (),
        parser: str = "json-line",
        perf_events: tuple[str, ...] = (),
        required: bool = True,
    ) -> dict[str, Any]:
        stdout_path = self.logs / f"{label}.stdout"
        stderr_path = self.logs / f"{label}.stderr"
        perf_path = self.logs / f"{label}.perf.csv"
        command = list(map(str, argv))
        if perf_events:
            if not sys.platform.startswith("linux"):
                raise RuntimeError("perf events are currently supported only on Linux")
            command = ["perf", "stat", "-x", ";", "-o", str(perf_path)] + [
                flag for event in perf_events for flag in ("-e", event)
            ] + ["--"] + command
        run_env = dict(self.base_env)
        if env:
            run_env.update(env)
        started = time.time()
        entry: dict[str, Any] = dict(
            label=label,
            argv=command,
            command=shlex.join(command),
            cwd=str(cwd) if cwd else None,
            cpus=list(cpus),
            started_unix=started,
        )
        self.commands.append(entry)
        preexec = None
        if cpus and hasattr(os, "sched_setaffinity"):
            cpu_set = set(cpus)

            def set_affinity() -> None:
                os.sched_setaffinity(0, cpu_set)

            preexec = set_affinity
        creationflags = 0
        start_new_session = os.name != "nt"
        if os.name == "nt" and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        try:
            with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
                child = subprocess.Popen(
                    command,
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                    cwd=cwd,
                    env=run_env,
                    preexec_fn=preexec,
                    start_new_session=start_new_session,
                    creationflags=creationflags,
                )
                try:
                    returncode = child.wait(timeout=self.timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    if os.name != "nt":
                        os.killpg(child.pid, signal.SIGKILL)
                    else:
                        child.kill()
                    child.wait()
                    returncode = -1
                    entry["timed_out"] = True
                    if required:
                        raise
            entry["returncode"] = returncode
            stdout_text = stdout_path.read_text(errors="replace")
            entry["stdout"] = str(stdout_path)
            entry["stderr"] = str(stderr_path)
            if returncode and required:
                raise RuntimeError(f"{label} failed with {returncode}; see {stderr_path}")
            if returncode == 0:
                parsed = _parse_result(stdout_text, parser)
                if parsed is not None:
                    if "status" in parsed and parsed["status"] != "ok":
                        raise RuntimeError(f"{label}: result status is {parsed['status']!r}")
                    entry["result"] = parsed
            if perf_events:
                entry["perf"] = parse_perf_stat(perf_path)
            return entry
        finally:
            entry["elapsed_seconds"] = time.time() - started
            save_json(self.output / "commands.json", self.commands)

    # Compatibility-sized primitive for project-specific adapters that want to
    # keep their own parsing/statistics while sharing lifecycle/logging.
    def command(
        self,
        argv: list[str],
        label: str,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        cpus: tuple[int, ...] = (),
        required: bool = True,
    ) -> tuple[int, str]:
        entry = self.run(
            argv, label, cwd=cwd, env=env, cpus=cpus, parser="none", required=required
        )
        return int(entry.get("returncode", 0)), Path(entry["stdout"]).read_text(errors="replace")


def summarize_runs(runs: list[dict[str, Any]], metrics: list[str], invariants: list[str]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in runs:
        if row.get("warmup"):
            continue
        groups.setdefault(row["case"], []).append(row)
    summaries: list[dict[str, Any]] = []
    for case, rows in groups.items():
        item: dict[str, Any] = dict(
            case=case,
            runs=len(rows),
            elapsed_seconds=dict(
                median=statistics.median(r["elapsed_seconds"] for r in rows),
                minimum=min(r["elapsed_seconds"] for r in rows),
                maximum=max(r["elapsed_seconds"] for r in rows),
            ),
            metadata=rows[0].get("case_metadata", {}),
        )
        parsed = [r.get("result") for r in rows if isinstance(r.get("result"), dict)]
        if parsed:
            selected_metrics = list(metrics)
            if not selected_metrics:
                for key, value in parsed[0].items():
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        selected_metrics.append(key)
            metric_summary: dict[str, Any] = {}
            for field in selected_metrics:
                values: list[float] = []
                for result in parsed:
                    try:
                        value = lookup_field(result, field)
                    except KeyError:
                        continue
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        values.append(float(value))
                if values:
                    metric_summary[field] = dict(
                        median=statistics.median(values), minimum=min(values), maximum=max(values)
                    )
            if metric_summary:
                item["metrics"] = metric_summary
            invariant_values: dict[str, Any] = {}
            for field in invariants:
                values = [lookup_field(result, field) for result in parsed]
                first = values[0]
                if any(value != first for value in values[1:]):
                    raise RuntimeError(f"case {case}: invariant {field!r} changed across runs: {values}")
                invariant_values[field] = first
            if invariant_values:
                item["invariants"] = invariant_values
        summaries.append(item)
    return summaries
