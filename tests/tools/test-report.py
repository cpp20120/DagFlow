#!/usr/bin/env python3
"""Read-only workflow journal formatter checks."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile


CLI = Path(sys.argv[1]).resolve()


def call(directory, expected=0):
    result = subprocess.run([str(CLI), "workflow", "report", str(directory)],
                            text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    assert result.returncode == expected, (result.returncode, result.stdout)
    return result.stdout


def report(status, nodes):
    return {"schemaVersion": 1, "workflow": "benchmarks", "manifestHash": "abc",
            "status": status, "nodes": nodes}


def node(identifier, kind, status, **extra):
    return {"id": identifier, "kind": kind, "status": status,
            "buildDirectory": "/tmp/build", "target": "dagflow_tests",
            "durationSeconds": 0.42, **extra}


def main(root):
    cases = [
        ("success", report("succeeded", [node("configure", "configure", "succeeded", **{
            "requestedContext": {"mode": "managed"}, "sessionAfter": {"provenance": "RecordedInvocation"}}),
            node("invoke", "invoke", "succeeded", **{"experimentResult": "succeeded"}),
            node("test", "ctest", "succeeded", **{"experimentResult": "succeeded"})])),
        ("failed", report("failed", [node("build", "build", "failed", **{"exitCode": 1,
            "stderr": "build.stderr.log", "error": "Process exited with code 1."}),
            node("run", "run", "blocked", **{"error": "A prerequisite did not succeed."})])),
        ("canceled", report("canceled", [node("run", "run", "canceled", **{
            "experimentResult": "canceled", "error": "Canceled."}),
            node("later", "invoke", "canceled")])),
        ("partial", report("failed", [node("done", "build", "succeeded", **{
            "requestedContext": {"mode": "adopt"},
            "buildVerification": {"verification": "BackendSucceeded", "target": {
                "buildDirectory": "/tmp/build", "target": "worker", "configuration": "Debug"},
                "backendExitCode": 0}}), node("pending", "run", "blocked")])),
    ]
    for name, payload in cases:
        directory = root / name
        directory.mkdir()
        path = directory / "run.json"
        path.write_text(json.dumps(payload))
        before = hashlib.sha256(path.read_bytes()).digest()
        existing = set(directory.iterdir())
        output = call(directory)
        assert path.read_bytes() and hashlib.sha256(path.read_bytes()).digest() == before
        assert set(directory.iterdir()) == existing, "report command mutated its input directory"
        assert "Workflow: benchmarks" in output
        assert f"Status: {payload['status'].upper()}" in output
        if name == "success":
            assert "experiment result: succeeded" in output
            assert "native build verification:" not in output, "invoke/CTest was mislabeled as native build verification"
        if name == "failed":
            assert "build" in output and "FAILED" in output and "exit code: 1" in output
            assert str(directory / "build.stderr.log") in output
        if name == "partial":
            assert "native build verification: BackendSucceeded" in output
            assert "Freshness: Unverified" in output
        if name == "canceled":
            assert "CANCELED" in output

    corrupt = root / "corrupt"
    corrupt.mkdir()
    corrupt_path = corrupt / "run.json"
    corrupt_path.write_text("{")
    assert "Cannot read workflow report" in call(corrupt, expected=1)
    corrupt_path.write_text(json.dumps({"workflow": "broken", "status": "failed", "nodes": [None]}))
    assert "Cannot read workflow report" in call(corrupt, expected=1)
    missing = root / "missing"
    missing.mkdir()
    assert "Cannot read workflow report" in call(missing, expected=1)
    print("All workflow report checks passed.", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="dagflow-report-") as temporary:
        main(Path(temporary))
