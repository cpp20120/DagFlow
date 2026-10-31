#!/usr/bin/env python3
"""CMake ownership/state regressions with real temporary projects (no mocks).

Usage: python3 tests/test-cmake-boundary.py /path/to/DagFlow.Tools
"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

spec = importlib.util.spec_from_file_location("workflow_tests", Path(__file__).with_name("test-workflow.py"))
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
CLI = suite.CLI
command, workflow, states, log, plan_error = suite.command, suite.workflow, suite.states, suite.log, suite.plan_error


def fingerprint(root):
    return {str(p.relative_to(root)): (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in root.rglob("*") if p.is_file()}


def inspect(build):
    return json.loads(command([CLI, "context", "inspect", build]))


def preflight_rejected(root, document, name, fragment):
    manifest = root / (name + ".json")
    manifest.write_text(json.dumps(document))
    results = root / (name + "-results")
    response = command([CLI, "workflow", "run", manifest, "--results-dir", results], expected=1)
    assert fragment in response and "Preflight rejected" in response, response
    assert not results.exists(), "Known-invalid graph produced a results directory"
    return response


def main(root):
    source = root / "source"
    source.mkdir()
    escaped_cli = str(CLI).replace("\\", "/").replace('"', '\\"')
    (source / "CMakeLists.txt").write_text('''cmake_minimum_required(VERSION 3.26)
project(BoundaryProbe LANGUAGES CXX)
file(APPEND "${CMAKE_BINARY_DIR}/configure-count.txt" "configure\\n")
file(WRITE "${CMAKE_BINARY_DIR}/preset-env.txt" "$ENV{PRESET_ENV}")
if(FAIL_CONFIGURE)
  message(FATAL_ERROR "REQUESTED_CONFIGURE_FAILURE")
endif()
if(SLOW_CONFIGURE)
  file(WRITE "${CMAKE_BINARY_DIR}/configuring" "started")
  execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 30)
endif()
add_executable(worker worker.cpp)
add_library(native_library STATIC library.cpp)
add_custom_target(fuzz_campaign
  COMMAND "${CMAKE_COMMAND}" -E touch "${CMAKE_BINARY_DIR}/CAMPAIGN_RAN")
file(WRITE "${CMAKE_BINARY_DIR}/replacement.bin" "BROKEN_EXECUTABLE")
add_custom_target(clobber_worker
  COMMAND "${CMAKE_COMMAND}" -E copy "${CMAKE_BINARY_DIR}/replacement.bin" "$<TARGET_FILE:worker>"
  DEPENDS worker)
add_custom_target(nested
  COMMAND "''' + escaped_cli + '''" target build --build-dir "${CMAKE_BINARY_DIR}" --target worker)
add_executable(prepared_worker prepared.cpp)
add_custom_target(prepared_nested
  COMMAND "''' + escaped_cli + '''" target run --prepared --build-dir "${CMAKE_BINARY_DIR}"
    --target prepared_worker --config "$<CONFIG>" -- "''' + escaped_cli + '''" "${CMAKE_BINARY_DIR}"
  DEPENDS prepared_worker VERBATIM)
enable_testing()
add_test(NAME worker_test COMMAND worker)
''')
    (source / "worker.cpp").write_text('#include <iostream>\nint main() { std::cout << "WORKER_RAN\\n"; }\n')
    (source / "library.cpp").write_text('int library_function() { return 1; }\n')
    (source / "prepared.cpp").write_text(r'''#include <cstdlib>
#include <string>
int main(int argc, char** argv) {
  if (argc != 3) return 2;
  std::string command = "\"" + std::string(argv[1]) + "\" target build --build-dir \"" + argv[2] + "\" --target worker";
  return std::system(command.c_str()) == 0 ? 0 : 1;
}
''')
    presets = {"version": 6, "configurePresets": [
        {"name": "base", "hidden": True, "generator": "Ninja", "environment": {"PRESET_ENV": "from-cmake-preset"}},
        {"name": "debug", "inherits": "base", "cacheVariables": {"CMAKE_BUILD_TYPE": "Debug", "FAIL_CONFIGURE": "OFF", "SLOW_CONFIGURE": "OFF"}},
        {"name": "same-cache", "inherits": "debug"},
        {"name": "release", "inherits": "debug", "cacheVariables": {"CMAKE_BUILD_TYPE": "Release"}},
        {"name": "failure", "inherits": "debug", "cacheVariables": {"FAIL_CONFIGURE": "ON"}},
        {"name": "slow", "inherits": "debug", "cacheVariables": {"SLOW_CONFIGURE": "ON"}},
        {"name": "multi", "inherits": "debug", "generator": "Ninja Multi-Config"},
    ]}
    (source / "CMakePresets.json").write_text(json.dumps(presets))
    build = root / "managed"
    doc = {"schemaVersion": 2, "contexts": {"worker": {
        "buildDirectory": "managed", "sourceDirectory": "source", "configurePreset": "debug", "configuration": "Debug"}},
        "nodes": [
            {"id": "configure", "kind": "configure", "context": "worker"},
            {"id": "build", "kind": "build", "context": "worker", "target": "worker", "dependsOn": ["configure"]},
            {"id": "run", "kind": "run", "from": "build"},
            {"id": "test", "kind": "ctest", "context": "worker", "dependsOn": ["run"]},
        ]}
    plan = root / "plan.json"
    plan.write_text(json.dumps(doc))
    before = fingerprint(root)
    text = command([CLI, "workflow", "plan", plan])
    assert "Unknown" in text and "Runtime" in text and "Unverified" in text
    assert inspect(build)["state"] == "Unknown"
    assert fingerprint(root) == before and not build.exists()
    print("PASS unknown/future context planning is completely read-only", flush=True)

    report, output = workflow(root, doc, name="configured")
    assert set(states(report).values()) == {"succeeded"}, report
    by_id = {n["id"]: n for n in report["nodes"]}
    assert by_id["configure"]["configure"]["state"] == "Succeeded"
    assert by_id["build"]["buildVerification"]["verification"] == "BackendSucceeded"
    assert by_id["run"]["artifact"]["materialization"] == "VerifiedAgainstBuild"
    assert by_id["run"]["experimentResult"] == "succeeded"
    observed = inspect(build)
    assert observed["freshness"] == "Unverified" and observed["presetState"] == "RecordedInvocation"
    assert observed["hasCacheReply"] and observed["toolchains"]
    assert (build / "preset-env.txt").read_text() == "from-cmake-preset"
    assert "WORKER_RAN" in log(report, output, "run")
    print("PASS explicit configure, opaque preset inheritance/environment and distinct result records", flush=True)

    existing = copy.deepcopy(doc)
    existing["nodes"] = existing["nodes"][1:]
    existing["nodes"][0]["dependsOn"] = []
    mismatch = copy.deepcopy(existing)
    mismatch["contexts"]["worker"]["configurePreset"] = "same-cache"
    plan_error(root, mismatch, "conflicts")
    mismatch["contexts"]["worker"].pop("configurePreset")
    mismatch["contexts"]["worker"]["configuration"] = "Release"
    plan_error(root, mismatch, "disagrees")
    preflight_rejected(root, mismatch, "mismatch", "disagrees")
    # An external configure invalidates recorded invocation matching even when
    # the resulting cache values happen to be the same.
    command(["cmake", "--preset", "same-cache", "-S", source, "-B", build])
    plan_error(root, existing, "Unverified")
    # Deliberate adoption does not infer the requested preset from cache values.
    adopted = copy.deepcopy(existing)
    adopted["contexts"]["worker"]["mode"] = "adopt"
    adopted["contexts"]["worker"]["provenancePolicy"] = "allowUnverified"
    report, _ = workflow(root, adopted, name="explicit-adoption")
    assert report["status"] == "succeeded"
    assert report["nodes"][0]["observedContext"]["presetState"] == "GenerationChanged"
    report, _ = workflow(root, doc, name="restore-provenance")
    assert report["status"] == "succeeded"
    print("PASS requested preset/configuration is never guessed from cache equality", flush=True)

    campaign = copy.deepcopy(existing)
    campaign["nodes"] = [{"id": "prepare", "kind": "build", "context": "worker", "target": "fuzz_campaign"},
                         {"id": "run", "kind": "run", "from": "prepare"}]
    plan_error(root, campaign, "invoke")
    preflight_rejected(root, campaign, "unsafe-build", "invoke")
    # A known-invalid independent branch must stop the entire workflow, including
    # the branch with an expensive explicit campaign.
    dangerous = copy.deepcopy(campaign)
    dangerous["nodes"].append({"id": "independent-campaign", "kind": "invoke",
                               "context": "worker", "target": "fuzz_campaign"})
    assert not (build / "CAMPAIGN_RAN").exists()
    preflight_rejected(root, dangerous, "unsafe-mixed-branches", "invoke")
    assert not (build / "CAMPAIGN_RAN").exists()
    for verb in ["build", "run"]:
        text = command([CLI, "target", verb, "--build-dir", build, "--target", "fuzz_campaign"], expected=1)
        assert "UTILITY" in text and "invoke" in text
    text = command([CLI, "workflow", "sequence", "--build-dir", build,
                    "--first", "fuzz_campaign", "--second", "worker"], expected=1)
    assert "UTILITY" in text and not (build / "CAMPAIGN_RAN").exists()
    old = copy.deepcopy(campaign)
    old["schemaVersion"] = 1
    plan_error(root, old, "Migrate")
    explicit = copy.deepcopy(campaign)
    explicit["nodes"] = [{"id": "campaign", "kind": "invoke", "context": "worker", "target": "fuzz_campaign"}]
    plan.write_text(json.dumps(explicit))
    before = fingerprint(build)
    text = command([CLI, "workflow", "plan", plan])
    assert "arbitrary commands" in text and fingerprint(build) == before
    report, _ = workflow(root, explicit, name="intentional-campaign")
    assert states(report)["campaign"] == "succeeded" and (build / "CAMPAIGN_RAN").exists()
    assert report["nodes"][0]["buildVerification"] is None
    (build / "CAMPAIGN_RAN").unlink()
    command([CLI, "target", "invoke", "--build-dir", build, "--target", "fuzz_campaign"])
    assert (build / "CAMPAIGN_RAN").exists()
    command([CLI, "target", "build", "--build-dir", build, "--target", "native_library"])
    print("PASS native Build versus explicit Invoke; campaigns never run during executable preparation", flush=True)

    # Missing File API is a real configured project, not a mocked CMake response.
    limited = root / "limited"
    command(["cmake", "--preset", "debug", "-S", source, "-B", limited])
    limited_doc = copy.deepcopy(existing)
    limited_doc["contexts"]["worker"] = {"buildDirectory": "limited", "configuration": "Debug", "mode": "adopt"}
    plan_error(root, limited_doc, "Unknown")
    preflight_rejected(root, limited_doc, "unknown-target", "Unknown")
    assert not (limited / "worker").exists()
    observed = inspect(limited)
    assert observed["state"] == "ObservedLimited" and observed["presetState"] == "Unknown"
    # An explicit configure adds queries and enables subsequent type validation.
    limited_doc["contexts"]["worker"].update(sourceDirectory="source", configurePreset="debug")
    limited_doc["contexts"]["worker"]["mode"] = "managed"
    limited_doc["nodes"].insert(0, doc["nodes"][0])
    limited_doc["nodes"][1]["dependsOn"] = ["configure"]
    report, _ = workflow(root, limited_doc, name="populate-api")
    assert report["status"] == "succeeded"
    cache_path = limited / "CMakeCache.txt"
    cache_text = cache_path.read_text()
    cache_path.write_text(cache_text.replace("CMAKE_BUILD_TYPE:STRING=Debug", "CMAKE_BUILD_TYPE:STRING=Release"))
    observed = inspect(limited)
    assert observed["state"] == "Unknown" and any("disagree" in m for m in observed["diagnostics"])
    cache_path.write_text(cache_text)
    print("PASS missing target metadata blocks implicit builds; explicit configure repairs observation", flush=True)

    # Valid bridge metadata remains usable without File API, but cannot override a
    # real newer codemodel where the target has become a UTILITY target.
    bridge = root / "bridge"
    command(["cmake", "-S", suite.ROOT / "tests/tools/fixture", "-B", bridge, "-G", "Ninja",
             "-DCMAKE_BUILD_TYPE=Debug", f"-DDAGFLOW_TOOLS_EXECUTABLE={CLI}"])
    command(["cmake", "--build", bridge, "--target", "dagflow_bridge_fuzz_queues"])
    assert any(t["evidence"] == "BridgeManifest" for t in inspect(bridge)["targets"])
    stale = build / "dagflow-tools/targets/Debug/fuzz_campaign.json"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text(json.dumps({"schemaVersion": 1, "target": "fuzz_campaign", "configuration": "Debug", "executable": str(build / "worker")}))
    text = command([CLI, "target", "run", "--build-dir", build, "--target", "fuzz_campaign"], expected=1)
    assert "UTILITY" in text
    print("PASS limited bridge compatibility and current codemodel priority", flush=True)

    failed = copy.deepcopy(doc)
    failed["contexts"]["worker"]["configurePreset"] = "failure"
    report, _ = workflow(root, failed, expected=1, name="failed-configure")
    assert states(report)["configure"] == "failed" and states(report)["build"] == "blocked"
    assert inspect(build)["state"] == "ConfigureFailed"
    # A retry is explicit, never an automatic clean/fresh operation.
    report, _ = workflow(root, doc, name="explicit-reconfigure")
    assert report["status"] == "succeeded"
    # A requested configuration that the preset does not produce is an error.
    wrong = copy.deepcopy(doc)
    wrong["contexts"]["worker"].update(buildDirectory="wrong-config", configuration="Release")
    report, _ = workflow(root, wrong, expected=1, name="wrong-config")
    assert states(report)["configure"] == "failed" and states(report)["build"] == "blocked"
    print("PASS configure failures/mismatches never bless stale metadata", flush=True)

    multi = copy.deepcopy(doc)
    multi["contexts"]["worker"].update(buildDirectory="multi", configurePreset="multi", configuration="Release")
    report, _ = workflow(root, multi, name="multi")
    assert report["status"] == "succeeded" and (root / "multi/Release/worker").exists()
    configure_only = copy.deepcopy(multi)
    configure_only["contexts"]["worker"].pop("configuration")
    configure_only["nodes"] = [doc["nodes"][0]]
    report, _ = workflow(root, configure_only, name="configure-multi-only")
    assert report["status"] == "succeeded"
    print("PASS configure is independent of multi-config build selection", flush=True)

    # Ninja may regenerate CMake during --build. A new File API index must not
    # be called a configuration mismatch; strict refuses to claim preset provenance.
    regen = copy.deepcopy(doc)
    regen["contexts"]["worker"]["buildDirectory"] = "regen"
    report, _ = workflow(root, regen, name="initial-regeneration")
    assert report["status"] == "succeeded"
    (source / "CMakeLists.txt").write_text((source / "CMakeLists.txt").read_text() + "\n# force regeneration\n")
    regen_existing = copy.deepcopy(regen)
    regen_existing["nodes"] = regen_existing["nodes"][1:]
    regen_existing["nodes"][0]["dependsOn"] = []
    report, _ = workflow(root, regen_existing, expected=1, name="strict-regeneration")
    assert states(report) == {"build": "failed", "run": "blocked", "test": "blocked"}, states(report)
    assert "Unverified" in next(n for n in report["nodes"] if n["id"] == "build")["error"]
    assert inspect(root / "regen")["presetState"] == "GenerationChanged"
    allow = copy.deepcopy(regen_existing)
    allow["contexts"]["worker"]["provenancePolicy"] = "allowUnverified"
    report, _ = workflow(root, allow, name="adopt-regeneration")
    assert report["status"] == "succeeded", report
    assert next(n for n in report["nodes"] if n["id"] == "build")["observationAfter"]["presetState"] == "GenerationChanged"
    print("PASS automatic regeneration distinguishes uncertainty from mismatch", flush=True)

    # Snapshot the executable after Build, not just before Run. A utility target
    # between them may alter the binary; the runner must reject it without exec.
    clobber = copy.deepcopy(doc)
    clobber["contexts"]["worker"]["buildDirectory"] = "clobber"
    clobber["nodes"] = [
        clobber["nodes"][0],
        clobber["nodes"][1],
        {"id": "clobber", "kind": "invoke", "context": "worker", "target": "clobber_worker", "dependsOn": ["build"]},
        {"id": "run", "kind": "run", "from": "build", "dependsOn": ["clobber"]},
    ]
    report, output = workflow(root, clobber, expected=1, name="artifact-drift")
    assert states(report) == {"configure": "succeeded", "build": "succeeded", "clobber": "succeeded", "run": "failed"}
    assert "changed since" in next(n for n in report["nodes"] if n["id"] == "run")["error"]
    assert next(n for n in report["nodes"] if n["id"] == "build")["artifact"]["materialization"] == "VerifiedAfterBuild"
    assert not (output / "run.stdout.log").exists(), "Drifted executable was started"
    print("PASS executable drift detected between build and run", flush=True)

    nested = copy.deepcopy(explicit)
    # Earlier tests changed CMake inputs. These checks concern ownership, so
    # explicitly accept regeneration uncertainty instead of requiring a barrier.
    nested["contexts"]["worker"].update(mode="adopt", provenancePolicy="allowUnverified")
    nested["nodes"][0]["target"] = "nested"
    report, output = workflow(root, nested, expected=1, name="nested")
    assert states(report)["campaign"] == "failed"
    assert "Unsafe nested build" in log(report, output, "campaign") or "Unsafe nested build" in (output / "campaign.stderr.log").read_text()
    nested["nodes"][0]["target"] = "prepared_nested"
    nested["nodes"][0]["timeoutSeconds"] = 10
    report, output = workflow(root, nested, expected=1, name="prepared-nested")
    assert "Unsafe nested build" in log(report, output, "campaign") or "Unsafe nested build" in (output / "campaign.stderr.log").read_text()
    assert "Timed out" not in (report["nodes"][0]["error"] or "")
    print("PASS recursive CLI invocation fails rather than deadlocking its build tree", flush=True)

    slow = copy.deepcopy(doc)
    slow["contexts"]["worker"].update(buildDirectory="slow", configurePreset="slow")
    manifest = root / "slow.json"
    manifest.write_text(json.dumps(slow))
    results = root / "slow-results"
    with (root / "slow.log").open("w") as log_file:
        proc = suite.start_cli([str(CLI), "workflow", "run", str(manifest), "--results-dir", str(results)],
                               stdout=log_file, stderr=subprocess.STDOUT)
        contenders = []
        try:
            suite.wait_for(root / "slow/configuring", proc)
            for args in [
                ["target", "build", "--build-dir", str(root / "slow"), "--target", "worker"],
                ["workflow", "sequence", "--build-dir", str(root / "slow"), "--first", "worker", "--second", "worker"],
            ]:
                contenders.append(suite.start_cli([str(CLI), *args], stdout=log_file,
                                                  stderr=subprocess.STDOUT))
            time.sleep(0.3)
            assert all(p.poll() is None for p in contenders), "Legacy command bypassed workflow ownership"
            suite.cancel_cli(proc)
            assert proc.wait(timeout=10) == 130
            assert all(p.wait(timeout=10) == 1 for p in contenders)
        finally:
            for child in [proc, *contenders]:
                suite.kill_cli(child)
    report = json.loads(next(results.glob("*/run.json")).read_text())
    assert states(report)["configure"] == "canceled" and states(report)["build"] == "canceled"
    observed = inspect(root / "slow")
    assert observed["state"] in ("Unknown", "ConfigureFailed")
    recovered = copy.deepcopy(doc)
    recovered["contexts"]["worker"]["buildDirectory"] = "slow"
    report, _ = workflow(root, recovered, name="recover-after-cancel")
    assert report["status"] == "succeeded"
    print("PASS configure cancellation, failed-state observation and explicit recovery", flush=True)


with tempfile.TemporaryDirectory(prefix="dagflow-boundary-") as temporary:
    main(Path(temporary))
print("All CMake boundary integration checks passed.", flush=True)
