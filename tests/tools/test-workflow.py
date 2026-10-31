#!/usr/bin/env python3
"""Integration tests against a published CLI, real CMake/Ninja and native workers.

Usage: python3 tests/test-workflow.py /path/to/DagFlow.Tools
Python is used only for this test driver. All fixtures are generated in a fresh
temporary directory; no project CMake files or existing build trees are changed.
"""

import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


CLI = Path(sys.argv[1]).resolve()
ROOT = Path(__file__).resolve().parents[2]


def command(args, expected=0, timeout=60):
    process = subprocess.run([str(a) for a in args], text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             timeout=timeout)
    assert process.returncode == expected, (args, process.returncode, process.stdout)
    return process.stdout


def configure(source, build, multi=False, bridge=False):
    query = build / ".cmake/api/v1/query"
    query.mkdir(parents=True)
    (query / "codemodel-v2").touch()
    args = ["cmake", "-S", source, "-B", build, "-G",
            "Ninja Multi-Config" if multi else "Ninja", "-DCMAKE_BUILD_TYPE=Debug"]
    if bridge:
        args.append(f"-DDAGFLOW_TOOLS_EXECUTABLE={CLI.as_posix()}")
    command(args)


def workflow(root, document, *options, expected=0, name="workflow"):
    manifest = root / f"{name}.json"
    manifest.write_text(json.dumps(document))
    results = root / (name + "-results")
    command([CLI, "workflow", "run", manifest, "--results-dir", results, *options], expected)
    reports = sorted(results.glob("*/run.json"))
    assert reports, f"No report in {results}"
    report = reports[-1]
    return json.loads(report.read_text()), report.parent


def states(report):
    return {n["id"]: n["status"] for n in report["nodes"]}


def log(report, output, node):
    return (output / next(n["stdout"] for n in report["nodes"] if n["id"] == node)).read_text()


def plan_error(root, document, fragment, *options):
    manifest = root / "invalid.json"
    manifest.write_text(json.dumps(document))
    output = command([CLI, "workflow", "plan", manifest, *options], expected=1)
    assert fragment in output, output


def wait_for(path, process, timeout=15):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if path.exists() and path.read_text():
            return
        assert process.poll() is None, "CLI exited before worker became ready"
        time.sleep(0.03)
    raise AssertionError(f"Timeout waiting for {path}")


def start_cli(args, **kwargs):
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(args, **kwargs)


def cancel_cli(process):
    if os.name == "nt":
        process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        process.send_signal(signal.SIGINT)


def kill_cli(process):
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if process.poll() is None:
            process.kill()
    else:
        os.killpg(process.pid, signal.SIGKILL)
    process.wait()


def main(root):
    source = root / "source"
    source.mkdir()
    (source / "CMakeLists.txt").write_text('''cmake_minimum_required(VERSION 3.26)
project(OrchestrationProbe LANGUAGES CXX)
enable_testing()
add_executable(first first.cpp)
add_executable(second second.cpp)
target_include_directories(second PRIVATE "${CMAKE_BINARY_DIR}")
set_target_properties(first PROPERTIES OUTPUT_NAME "first worker")
set_target_properties(second PROPERTIES OUTPUT_NAME "second worker")
add_custom_target(existing_action COMMAND "${CMAKE_COMMAND}" -E echo "EXISTING_ACTION")
add_test(NAME native_second COMMAND second)
set_tests_properties(native_second PROPERTIES LABELS smoke)
''')
    (source / "first.cpp").write_text(r'''#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif
int main(int argc, char** argv) {
  std::string mode = argc > 1 ? argv[1] : "args";
  if (mode == "fail") { std::cerr << "EXPECTED_FAILURE\n"; return 7; }
  if (mode == "generate") {
    std::ofstream("generated.hpp") << "#define GENERATED_VALUE " << argv[2] << "\n";
    return 0;
  }
  if (mode == "sleep") {
    { std::ofstream("ready.pid") << getpid(); }
    std::this_thread::sleep_for(std::chrono::milliseconds(std::stoi(argv[2])));
  }
  if (mode == "flood") {
    for (int i = 0; i < 20000; ++i) { std::cout << "stdout line\n"; std::cerr << "stderr line\n"; }
  }
  for (int i = 1; i < argc; ++i) std::cout << "[" << argv[i] << "]";
  if (auto env = std::getenv("WORKFLOW_TEST_VALUE")) std::cout << " ENV=" << env;
  std::cout << "\n";
}
''')
    (source / "second.cpp").write_text('''#include "generated.hpp"
#include <iostream>
int main() { std::cout << "VALUE=" << GENERATED_VALUE << "\\n"; }
''')
    build = root / "worker space"
    configure(source, build)
    doc = {
        "schemaVersion": 1,
        "contexts": {"worker": {"buildDirectory": "worker space", "mode": "adopt"}},
        "nodes": [
            {"id": "build-first", "kind": "build", "context": "worker", "target": "first"},
            {"id": "generate-one", "kind": "run", "from": "build-first", "arguments": ["generate", "1"]},
            {"id": "build-second", "kind": "build", "context": "worker", "target": "second", "dependsOn": ["generate-one"]},
            {"id": "run-one", "kind": "run", "from": "build-second"},
            {"id": "generate-two", "kind": "run", "from": "build-first", "dependsOn": ["run-one"], "arguments": ["generate", "2"]},
            {"id": "rebuild-second", "kind": "build", "context": "worker", "target": "second", "dependsOn": ["generate-two"]},
            {"id": "run-two", "kind": "run", "from": "rebuild-second", "exclusive": True},
            {"id": "check", "kind": "ctest", "context": "worker", "dependsOn": ["run-two"], "labelRegex": "smoke"},
            {"id": "action", "kind": "invoke", "context": "worker", "target": "existing_action", "dependsOn": ["check"]},
        ],
    }
    manifest = root / "plan.json"
    manifest.write_text(json.dumps(doc))
    command([CLI, "workflow", "plan", manifest, "--only", "run-one"])
    assert not (build / "first worker").exists(), "plan unexpectedly built a target"
    assert not (build / "dagflow-tools/workflow.lock").exists(), "plan unexpectedly acquired a lease"
    print("PASS read-only planning", flush=True)

    # This cannot work if the scheduler hoists all builds before all executions.
    report, output = workflow(root, doc, "--jobs", "4")
    assert set(states(report).values()) == {"succeeded"}, report
    assert "VALUE=1" in log(report, output, "run-one")
    assert "VALUE=2" in log(report, output, "run-two")
    assert "EXISTING_ACTION" in log(report, output, "action")
    assert all(n["configuration"] == "Debug" for n in report["nodes"])
    assert all(n["executableSha256"] for n in report["nodes"] if n["kind"] == "run")
    print("PASS interleaved builds, repeated target invalidation, custom targets and CTest", flush=True)

    before = (build / "first worker").stat().st_mtime_ns
    report, output = workflow(root, doc, "--only", "build-first", name="incremental")
    assert states(report) == {"build-first": "succeeded"}
    assert (build / "first worker").stat().st_mtime_ns == before
    assert "no work to do" in log(report, output, "build-first")
    print("PASS selected dependency closure and native incremental no-op", flush=True)

    run = {"id": "run", "kind": "run", "from": "build-first", "arguments": ["args", "hello world", "", "a;b", '"quoted"'],
           "environment": {"WORKFLOW_TEST_VALUE": "value with spaces"}}
    simple = {**doc, "nodes": [doc["nodes"][0], run]}
    report, output = workflow(root, simple, name="arguments")
    assert '[hello world][][a;b]["quoted"] ENV=value with spaces' in log(report, output, "run")
    flood = copy.deepcopy(simple)
    flood["nodes"][1]["arguments"] = ["flood"]
    report, output = workflow(root, flood, name="flood")
    assert log(report, output, "run").count("stdout line") == 20000
    assert (output / "run.stderr.log").read_text().count("stderr line") == 20000
    print("PASS argument/environment fidelity and concurrent stdout/stderr draining", flush=True)

    failed = copy.deepcopy(simple)
    failed["nodes"][1]["arguments"] = ["fail"]
    failed["nodes"] += [
        {"id": "blocked", "kind": "invoke", "context": "worker", "target": "existing_action", "dependsOn": ["run"]},
        {"id": "independent", "kind": "invoke", "context": "worker", "target": "existing_action"},
    ]
    report, output = workflow(root, failed, "--jobs", "4", expected=1, name="failure")
    assert states(report) == {"build-first": "succeeded", "run": "failed", "blocked": "blocked", "independent": "succeeded"}
    assert next(n for n in report["nodes"] if n["id"] == "run")["exitCode"] == 7
    assert "EXPECTED_FAILURE" in (output / "run.stderr.log").read_text()
    print("PASS failure propagation and independent branch completion", flush=True)

    failed_build = copy.deepcopy(simple)
    failed_build["nodes"][0]["target"] = "nonexistent-target"
    plan_error(root, failed_build, "absent")
    # A known missing target is now rejected at preflight. Exercise an actual
    # backend failure separately, preserving failure propagation coverage.
    original = (source / "first.cpp").read_text()
    (source / "first.cpp").write_text("#error EXPECTED_BUILD_FAILURE\n")
    try:
        report, _ = workflow(root, simple, expected=1, name="failed-build")
    finally:
        (source / "first.cpp").write_text(original)
    assert states(report) == {"build-first": "failed", "run": "blocked"}

    no_tests = {**doc, "nodes": [doc["nodes"][2] | {"dependsOn": []},
        {"id": "empty", "kind": "ctest", "context": "worker", "dependsOn": ["build-second"], "testRegex": "does-not-exist"}]}
    report, _ = workflow(root, no_tests, expected=1, name="no-tests")
    assert states(report)["empty"] == "failed"

    invalid = copy.deepcopy(simple)
    invalid["nodes"][0]["dependsOn"] = ["run"]
    plan_error(root, invalid, "cycle")
    invalid = copy.deepcopy(simple)
    invalid["nodes"][1]["from"] = "missing"
    plan_error(root, invalid, "from must refer")
    invalid = copy.deepcopy(simple)
    invalid["nodes"][1]["argumnts"] = []
    plan_error(root, invalid, "argumnts")
    invalid = copy.deepcopy(simple)
    invalid["nodes"].append(copy.deepcopy(invalid["nodes"][0]))
    plan_error(root, invalid, "unique")
    plan_error(root, simple, "Unknown selected node", "--only", "missing")
    invalid = {**doc, "nodes": [{"id": "unbuilt", "kind": "ctest", "context": "worker"}]}
    plan_error(root, invalid, "build ancestor")
    plan_error(root, simple, "Unsafe nested build", "--controller-build-dir", build)
    alias = root / "worker-alias"
    if os.name == "nt":
        subprocess.run(["cmd.exe", "/d", "/c", f'mklink /J "{alias}" "{build}"'],
                       check=True, capture_output=True, text=True)
    else:
        alias.symlink_to(build, target_is_directory=True)
    if os.name == "nt" or alias.exists():
        plan_error(root, simple, "Unsafe nested build", "--controller-build-dir", alias)
    print("PASS graph validation, empty test selection and controller guard", flush=True)

    multi = root / "multi"
    configure(source, multi, multi=True)
    multi_doc = copy.deepcopy(simple)
    multi_doc["contexts"]["worker"]["buildDirectory"] = "multi"
    plan_error(root, multi_doc, "Multi-config build")
    multi_doc["contexts"]["worker"]["configuration"] = "Release"
    report, output = workflow(root, multi_doc, name="multi-config")
    assert set(states(report).values()) == {"succeeded"}
    assert all(n["configuration"] == "Release" for n in report["nodes"])
    assert (multi / "Release/first worker").exists()
    print("PASS multi-config artifact resolution", flush=True)

    # Reuse the existing bridge fixture unchanged to protect --prepared behavior.
    bridge = root / "bridge"
    configure(ROOT / "tests/tools/fixture", bridge, bridge=True)
    text = command(["cmake", "--build", bridge, "--target", "dagflow_bridge_fuzz_queues"])
    assert "fixture executable:" in text
    sequence = command([CLI, "workflow", "sequence", "--build-dir", build,
                        "--first", "first", "--second", "second"])
    assert "sequence completed" in sequence
    print("PASS existing prepared bridge and sequence commands", flush=True)

    timeout = copy.deepcopy(simple)
    timeout["nodes"][1].update(arguments=["sleep", "30000"], timeoutSeconds=0.3)
    report, _ = workflow(root, timeout, expected=1, name="timeout")
    node = next(n for n in report["nodes"] if n["id"] == "run")
    assert node["status"] == "failed" and "Timed out" in node["error"]
    if sys.platform == "linux":
        assert not Path("/proc", (build / "ready.pid").read_text()).exists()
    print("PASS timeout terminates worker", flush=True)

    # Different contexts may run concurrently; an exclusive benchmark waits for
    # all active nodes. Same physical build trees are serialized automatically.
    parallel = copy.deepcopy(simple)
    parallel["contexts"]["other"] = {"buildDirectory": "multi", "configuration": "Release", "mode": "adopt"}
    parallel["nodes"] = [
        doc["nodes"][0],
        {"id": "build-other", "kind": "build", "context": "other", "target": "first"},
        {"id": "sleep-one", "kind": "run", "from": "build-first", "dependsOn": ["build-other"], "arguments": ["sleep", "200"]},
        {"id": "sleep-two", "kind": "run", "from": "build-other", "dependsOn": ["build-first"], "arguments": ["sleep", "600"]},
        {"id": "benchmark", "kind": "run", "from": "build-first", "dependsOn": ["sleep-one"], "arguments": ["args"], "exclusive": True},
    ]
    report, _ = workflow(root, parallel, "--jobs", "4", name="parallel")
    by_id = {n["id"]: n for n in report["nodes"]}
    one, two = by_id["sleep-one"], by_id["sleep-two"]
    assert max(one["startedAt"], two["startedAt"]) < min(one["finishedAt"], two["finishedAt"])
    assert by_id["benchmark"]["startedAt"] >= max(one["finishedAt"], two["finishedAt"])
    for n in parallel["nodes"]:
        if n["kind"] == "run":
            n["resources"] = ["shared-device"]
    report, _ = workflow(root, parallel, "--jobs", "4", name="resources")
    by_id = {n["id"]: n for n in report["nodes"]}
    one, two = by_id["sleep-one"], by_id["sleep-two"]
    assert one["finishedAt"] <= two["startedAt"] or two["finishedAt"] <= one["startedAt"]
    print("PASS parallel branches, exclusive nodes and named resources", flush=True)

    aliased = copy.deepcopy(parallel)
    aliased["contexts"]["other"] = {"buildDirectory": "worker-alias", "mode": "adopt"}
    for n in aliased["nodes"]:
        n.pop("resources", None)
    report, _ = workflow(root, aliased, "--jobs", "4", name="same-tree")
    by_id = {n["id"]: n for n in report["nodes"]}
    one, two = by_id["sleep-one"], by_id["sleep-two"]
    assert one["finishedAt"] <= two["startedAt"] or two["finishedAt"] <= one["startedAt"]
    print("PASS same-tree serialization through physical aliases", flush=True)
    if os.name == "nt":
        alias.rmdir()
    else:
        alias.unlink()

    waiting = copy.deepcopy(simple)
    waiting["nodes"][1]["arguments"] = ["sleep", "30000"]
    waiting["nodes"].append({"id": "after", "kind": "invoke", "context": "worker",
                             "target": "existing_action", "dependsOn": ["run"]})
    manifest = root / "waiting.json"
    manifest.write_text(json.dumps(waiting))
    ready = build / "ready.pid"
    ready.unlink(missing_ok=True)
    results = root / "cancel-results"
    with (root / "cancel.log").open("w") as log_file:
        process = start_cli([str(CLI), "workflow", "run", str(manifest), "--results-dir", str(results)],
                            stdout=log_file, stderr=subprocess.STDOUT)
        contender = None
        try:
            wait_for(ready, process)
            pid = ready.read_text()
            # A second CLI must wait on the same tree, not start Ninja.
            contender_results = root / "contender-results"
            contender = start_cli([str(CLI), "workflow", "run", str(manifest), "--only", "build-first",
                                   "--results-dir", str(contender_results)],
                                  stdout=log_file, stderr=subprocess.STDOUT)
            time.sleep(0.4)
            assert contender.poll() is None, "Concurrent CLI bypassed tree lease"
            cancel_cli(process)
            assert process.wait(timeout=10) == 130
            assert contender.wait(timeout=15) == 0
            if sys.platform == "linux":
                assert not Path("/proc", pid).exists()
        finally:
            for child in [process, contender]:
                if child is not None:
                    kill_cli(child)
    report = json.loads(next(results.glob("*/run.json")).read_text())
    assert report["status"] == "canceled" and states(report)["after"] == "canceled"
    print("PASS console cancellation, dependent cancellation and cross-process tree lease", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="dagflow-workflow-") as temporary:
        main(Path(temporary))
    print("All workflow integration checks passed.", flush=True)
