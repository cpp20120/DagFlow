"""Small correctness/schema checks, never performance thresholds."""
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile

binary, runner = map(Path, sys.argv[1:])
names = subprocess.check_output([binary, "--list"], text=True).splitlines()
assert len(names) == len(set(names)) == 14
checksums = {}
for workers in (1, 2):
    for tasks in (1, 65, 513):
        for latency in (False, True):
            args = [binary, "--workers", str(workers), "--tasks", str(tasks),
                    "--iterations", "3", "--repeats", "2", "--warmup", "1",
                    "--sample-stride", "4", "--idle-us", "100"]
            if latency:
                args.append("--latency")
            rows = [json.loads(line) for line in subprocess.check_output(args, text=True, timeout=45).splitlines()]
            assert [r["scenario"] for r in rows] == names
            for row in rows:
                assert row["schema"] == 1
                if row["status"] == "skipped":
                    assert workers == 1 and row["scenario"] == "steal_heavy"
                    continue
                assert row["status"] == "ok"
                assert len(row["run_us"]) == 2 and all(t > 0 for t in row["run_us"])
                assert row["run_p50_us"] == min(row["run_us"])
                assert row["run_p999_us"] == max(row["run_us"])
                assert row["run_p50_us"] <= row["run_p99_us"] <= row["run_p999_us"]
                assert math.isfinite(row["payload_tasks_per_second"]) and row["payload_tasks_per_second"] > 0
                assert row["latency_samples"] == (2 * ((tasks - 1) // 4 + 1) if latency else 0)
                if latency:
                    assert 0 <= row["latency_p50_us"] <= row["latency_p99_us"] <= row["latency_p999_us"]
                else:
                    assert row["latency_p50_us"] is None
                key = (tasks, row["scenario"] == "uneven")
                assert checksums.setdefault(key, row["checksum"]) == row["checksum"]
# One worker guarantees a local backlog and reaches the central overflow path.
row = json.loads(subprocess.check_output(
    [binary, "--scenario", "local_saturated", "--workers", "1", "--tasks", "20000",
     "--repeats", "1", "--warmup", "0"], text=True, timeout=45))
assert row["status"] == "ok" and row["tasks"] == 20000
for shards in (1, 5):
    rows = [json.loads(line) for line in subprocess.check_output(
        [binary, "--workers", "2", "--shards", str(shards), "--tasks", "65",
         "--repeats", "1", "--warmup", "0"], text=True, timeout=45).splitlines()]
    assert len(rows) == len(names) and all(r["status"] == "ok" and r["shards"] == shards for r in rows)
for workers in (1, 4):
    for batch in (1, 4, 16, 64):
        row = json.loads(subprocess.check_output(
            [binary, "--scenario", "external_detached", "--workers", str(workers),
             "--submit-batch", str(batch), "--central-batch", "4", "--tasks", "137",
             "--iterations", "3", "--latency", "--sample-stride", "1",
             "--repeats", "2", "--warmup", "0"], text=True, timeout=45))
        assert row["submit_batch"] == batch and row["central_batch"] == 4
        assert row["latency_samples"] == 274
        assert checksums.setdefault((137, False), row["checksum"]) == row["checksum"]
for arguments in (["--workers", "0"], ["--repeats", "0"], ["--tasks", "-1"],
                  ["--submit-batch", "65"], ["--central-batch", "0"],
                  ["--scenario", "invalid"], ["--tasks"], ["--sample-stride", "0"],
                  ["--workers", "4294967296"], ["--work-ns", "junk"]):
    assert subprocess.run([binary, *arguments], capture_output=True, timeout=10).returncode != 0
with tempfile.TemporaryDirectory(prefix="dagflow-suite-test-") as temporary:
    output = Path(temporary) / "results"
    command = [sys.executable, runner, "--binary", binary, "--workers", "1,2",
               "--tasks", "33", "--work-ns", "0,100", "--repeats", "2", "--warmup", "0",
               "--scenarios", "scope_recursive,steal_heavy", "--output", output]
    subprocess.run(command, check=True, capture_output=True, text=True, timeout=45)
    rows = [json.loads(line) for line in (output / "results.jsonl").read_text().splitlines()]
    assert len(rows) == 16
    assert (output / "results.csv").is_file()
    assert (output / "manifest.json").is_file()
    # Prevent accidental overwrite of an existing measurement set.
    assert subprocess.run(command, capture_output=True, timeout=10).returncode != 0
print("benchmark scenarios, JSON schema, CLI and runner checks passed")
