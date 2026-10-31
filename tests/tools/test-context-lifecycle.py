#!/usr/bin/env python3
"""BuildContextSession integration: actual CMake/Ninja, no mocked build backend."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

spec = importlib.util.spec_from_file_location("suite", Path(__file__).with_name("test-workflow.py"))
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
CLI = suite.CLI
command, workflow, states, log = suite.command, suite.workflow, suite.states, suite.log


def fingerprint(root):
    return {str(p.relative_to(root)): (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in root.rglob("*") if p.is_file()}


def inspect(build):
    return json.loads(command([CLI, "context", "inspect", build]))


def rejected(root, doc, fragment, name="rejected"):
    manifest = root / (name + ".json")
    manifest.write_text(json.dumps(doc))
    before = fingerprint(root)
    text = command([CLI, "workflow", "plan", manifest], expected=1)
    assert fragment in text, text
    results = root / (name + "-results")
    text = command([CLI, "workflow", "run", manifest, "--results-dir", results], expected=1)
    assert "Preflight rejected" in text and not results.exists(), text
    assert fingerprint(root) == before


def nodes(report):
    return {n["id"]: n for n in report["nodes"]}


def main(root):
    source = root / "source"
    source.mkdir()
    rules = source / "rules.cmake"
    original_rules = "add_executable(worker worker.cpp)\n"
    rules.write_text(original_rules)
    (source / "CMakeLists.txt").write_text('''cmake_minimum_required(VERSION 3.26)
project(Lifecycle LANGUAGES CXX)
file(APPEND "${CMAKE_BINARY_DIR}/configured" "configure\\n")
file(WRITE "${CMAKE_BINARY_DIR}/environment" "$ENV{LIFECYCLE_ENV}")
include(rules.cmake)
add_custom_target(campaign COMMAND "${CMAKE_COMMAND}" -E touch "${CMAKE_BINARY_DIR}/CAMPAIGN")
add_executable(slow slow.cpp)
add_custom_command(TARGET slow PRE_LINK
  COMMAND "${CMAKE_COMMAND}" -E touch "${CMAKE_BINARY_DIR}/building"
  COMMAND "${CMAKE_COMMAND}" -E sleep 30
  COMMAND "${CMAKE_COMMAND}" -E touch "${CMAKE_BINARY_DIR}/BUILD_COMPLETED")
''')
    (source / "worker.cpp").write_text(r'''#include <fstream>
#include <iostream>
#include <string>
#include <cstdlib>
int main(int argc, char** argv) {
  if (argc > 1 && std::string(argv[1]) == "write") { std::ofstream(argv[2]) << argv[3]; return 0; }
  if (argc > 1 && std::string(argv[1]) == "external") return std::system(argv[2]) == 0 ? 0 : 1;
  std::cout << "EXECUTED\n";
}
''')
    (source / "slow.cpp").write_text("int main() {}\n")
    (source / "CMakePresets.json").write_text(json.dumps({"version": 6, "configurePresets": [
        {"name": "base", "hidden": True, "generator": "Ninja", "environment": {"LIFECYCLE_ENV": "inherited"}},
        {"name": "debug", "inherits": "base", "cacheVariables": {"CMAKE_BUILD_TYPE": "Debug"}},
    ]}))
    build = root / "tree"
    context = {"buildDirectory": "tree", "sourceDirectory": "source", "configuration": "Debug",
               "configurePreset": "debug", "mode": "managed"}
    configure = {"id": "configure", "kind": "configure", "context": "w"}
    first = {"id": "build", "kind": "build", "context": "w", "target": "worker", "dependsOn": ["configure"]}
    run = {"id": "run", "kind": "run", "from": "build"}
    doc = {"schemaVersion": 2, "contexts": {"w": context}, "nodes": [configure, first, run]}

    observe = {"schemaVersion": 1, "contexts": {"w": {"buildDirectory": "absent", "mode": "observe"}},
               "nodes": [{"id": "inspect", "kind": "inspect", "context": "w"}]}
    manifest = root / "observe.json"
    manifest.write_text(json.dumps(observe))
    before = fingerprint(root)
    assert "mode=observe" in command([CLI, "workflow", "plan", manifest])
    assert inspect(root / "absent")["state"] == "Unknown"
    assert before == fingerprint(root) and not (root / "absent").exists()
    report, _ = workflow(root, observe, name="observe")
    assert report["status"] == "succeeded" and not (root / "absent").exists()
    assert nodes(report)["inspect"]["sessionBefore"] == nodes(report)["inspect"]["sessionAfter"]
    # Observation also works under a parent's ownership guard: acquiring a
    # mutation lease (or checking a controller guard) here would reject it.
    environment = os.environ | {"DAGFLOW_TOOLS_OWNED_TREES": json.dumps([str(root / "absent")])}
    observed_run = subprocess.run([str(CLI), "workflow", "run", str(manifest), "--results-dir", str(root / "observe-guard")],
                                  env=environment, text=True, capture_output=True)
    assert observed_run.returncode == 0, observed_run.stderr
    assert not (root / "absent").exists()
    print("PASS Observe missing tree: no creation, queries, configure or mutation lease", flush=True)

    # Real IDE-owned tree: File API query belongs to the IDE/test setup.
    suite.configure(source, build)
    adopted = copy.deepcopy(doc)
    adopted["contexts"]["w"]["mode"] = "adopt"
    adopted["nodes"] = [first | {"dependsOn": []}, run]
    before_configures = (build / "configured").read_text()
    before_queries = fingerprint(build / ".cmake/api/v1/query")
    for schema in (1, 2):
        adopted["schemaVersion"] = schema
        report, _ = workflow(root, adopted, name=f"adopt-schema-{schema}")
        assert nodes(report)["build"]["sessionBefore"]["provenance"] == "AdoptedUnverified"
        assert nodes(report)["run"]["observedContext"]["presetState"] == "Unknown"
    assert (build / "configured").read_text() == before_configures
    assert fingerprint(build / ".cmake/api/v1/query") == before_queries
    assert not (build / "dagflow-tools/configure.json").exists()
    implicit = copy.deepcopy(adopted)
    implicit["contexts"]["w"].pop("mode")
    implicit["contexts"]["w"]["provenancePolicy"] = "allowUnverified"
    rejected(root, implicit, "Explicitly set mode")
    for update, fragment in [({"sourceDirectory": "."}, "source"), ({"configuration": "Release"}, "disagrees"),
                             ({"generator": "Ninja Multi-Config"}, "generator")]:
        wrong = copy.deepcopy(adopted)
        wrong["contexts"]["w"].update(update)
        rejected(root, wrong, fragment)
        wrong["contexts"]["w"]["mode"] = "observe"
        wrong["nodes"] = [{"id": "inspect", "kind": "inspect", "context": "w"}]
        rejected(root, wrong, fragment)
    print("PASS explicit IDE adoption, unknown preset, schemas 1/2 and observable conflicts", flush=True)

    repeated = copy.deepcopy(doc)
    repeated["nodes"] += [first | {"id": "build2", "dependsOn": ["run"]}, run | {"id": "run2", "from": "build2"}]
    report, output = workflow(root, repeated, name="managed-repeated")
    by_id = nodes(report)
    assert all(n["status"] == "succeeded" for n in by_id.values())
    assert (build / "configured").read_text() == before_configures + "configure\n"
    assert (build / "environment").read_text() == "inherited"
    invocation = by_id["configure"]["configure"]
    assert invocation["arguments"] == ["--preset", "debug", "-S", str(source), "-B", str(build)]
    assert invocation["executable"] and invocation["workingDirectory"] == str(source)
    assert by_id["configure"]["sessionAfter"]["generationChange"] == "ExplicitConfigure"
    assert "no work to do" in log(report, output, "build2")
    print("PASS managed configure argv/environment and repeated builds without extra configure", flush=True)

    # Regenerate *inside* one workflow: a run produces a CMake input; no hoisting.
    for policy in ("strict", "allowUnverified"):
        rules.write_text(original_rules)
        regenerate = copy.deepcopy(repeated)
        regenerate["contexts"]["w"]["provenancePolicy"] = policy
        regenerate["nodes"][2]["arguments"] = ["write", str(rules), original_rules + "# equivalent regeneration\n"]
        report, output = workflow(root, regenerate, expected=1 if policy == "strict" else 0, name="regen-" + policy)
        by_id = nodes(report)
        assert by_id["build2"]["sessionAfter"]["generationChange"] == "ObservedDuringBuildRequest"
        assert not by_id["build2"]["sessionAfter"]["observedPropertiesChanged"]
        assert by_id["build2"]["sessionAfter"]["provenance"] == "Unverified"
        assert by_id["build2"]["observationAfter"]["presetState"] == "GenerationChanged"
        assert by_id["build2"]["buildVerification"]["verification"] == "BackendSucceeded"
        if policy == "strict":
            assert by_id["run2"]["status"] == "blocked" and not (output / "run2.stdout.log").exists()
            assert "Unverified" in by_id["build2"]["error"]
        else:
            assert "EXECUTED" in log(report, output, "run2")
        assert not (build / "CAMPAIGN").exists()
    print("PASS equivalent Ninja regeneration: separate generation/provenance and both policies", flush=True)

    rules.write_text(original_rules)
    invoked = copy.deepcopy(repeated)
    invoked["contexts"]["w"]["provenancePolicy"] = "allowUnverified"
    invoked["nodes"][2]["arguments"] = ["write", str(rules), original_rules + "# regenerate on invoke\n"]
    invoked["nodes"].insert(3, {"id": "invoke", "kind": "invoke", "context": "w", "target": "help", "dependsOn": ["run"]})
    invoked["nodes"][4]["dependsOn"] = ["invoke"]
    report, _ = workflow(root, invoked, name="invoke-regeneration")
    assert nodes(report)["invoke"]["sessionAfter"]["generationChange"] == "ObservedDuringTargetInvocation"
    assert nodes(report)["invoke"]["buildVerification"] is None
    assert not (build / "CAMPAIGN").exists()
    rules.write_text(original_rules)
    invoked["contexts"]["w"]["provenancePolicy"] = "strict"
    report, output = workflow(root, invoked, expected=1, name="invoke-regeneration-strict")
    assert nodes(report)["invoke"]["status"] == "failed"
    assert nodes(report)["invoke"]["experimentResult"] == "succeeded"
    assert nodes(report)["invoke"]["exitCode"] == 0
    assert nodes(report)["run2"]["status"] == "blocked" and not (output / "run2.stdout.log").exists()
    print("PASS Invoke generation observation remains separate from native Build verification", flush=True)

    for content, fragment in [("add_library(worker STATIC slow.cpp)\n", "target type"),
                              ("add_custom_target(worker)\n", "UTILITY"),
                              (original_rules + 'set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)\n', "configuration")]:
        rules.write_text(original_rules)
        changed = copy.deepcopy(repeated)
        changed["contexts"]["w"]["provenancePolicy"] = "allowUnverified"
        changed["nodes"][2]["arguments"] = ["write", str(rules), content]
        report, output = workflow(root, changed, expected=1, name="conflict-" + fragment.replace(" ", "-"))
        assert nodes(report)["build2"]["status"] == "failed" and nodes(report)["run2"]["status"] == "blocked"
        assert fragment in nodes(report)["build2"]["error"], nodes(report)["build2"]
        assert not (output / "run2.stdout.log").exists() and not (build / "CAMPAIGN").exists()
    print("PASS real regenerated target/configuration conflicts block dependent runs", flush=True)

    # Out-of-band configure is detected at the next operation, not attributed to
    # our backend request. CMake runs from the worker while no Ninja is active.
    for policy in ("strict", "allowUnverified"):
        rules.write_text(original_rules)
        external = copy.deepcopy(repeated)
        external["contexts"]["w"]["provenancePolicy"] = policy
        external["nodes"][2]["arguments"] = ["external", f'cmake --preset debug -S "{source}" -B "{build}"']
        report, output = workflow(root, external, expected=1 if policy == "strict" else 0, name="external-" + policy)
        assert nodes(report)["build2"]["sessionBefore"]["generationChange"] == "ExternalOrUnknown"
        if policy == "strict":
            assert not (output / "build2.stdout.log").exists()
        else:
            assert nodes(report)["run2"]["sessionAfter"]["provenance"] == "Unverified"
    print("PASS external configure is distinguished from generation observed during Build", flush=True)

    rules.write_text(original_rules)
    stale = copy.deepcopy(doc)
    stale["contexts"]["w"]["provenancePolicy"] = "allowUnverified"
    stale["nodes"] += [configure | {"id": "configure2", "dependsOn": ["run"]},
                       run | {"id": "stale-run", "dependsOn": ["configure2"]}]
    report, output = workflow(root, stale, expected=1, name="stale-verification")
    assert "Generation changed since" in nodes(report)["stale-run"]["error"]
    assert not (output / "stale-run.stdout.log").exists()
    print("PASS a new configure cannot reuse the old build verification even with unchanged executable", flush=True)

    # Configure failure overwrites the prior successful record, then recovery is explicit.
    rules.write_text('message(FATAL_ERROR "FAILURE")\n')
    report, _ = workflow(root, doc, expected=1, name="configure-failure")
    record = json.loads((build / "dagflow-tools/configure.json").read_text())
    assert record["state"] == "Failed" and nodes(report)["run"]["status"] == "blocked"
    rejected(root, adopted, "ConfigureFailed")
    rules.write_text(original_rules)
    report, _ = workflow(root, doc, name="recovery")
    assert report["status"] == "succeeded"
    # Successful CMake with incompatible requested identity is rejected too.
    wrong = copy.deepcopy(doc)
    wrong["contexts"]["w"]["configuration"] = "Release"
    report, _ = workflow(root, wrong, expected=1, name="rejected-configure")
    assert json.loads((build / "dagflow-tools/configure.json").read_text())["state"] == "Rejected"
    assert nodes(report)["configure"]["sessionAfter"]["provenance"] == "Unverified"
    assert nodes(report)["configure"]["sessionAfter"]["requiresConfigure"]
    workflow(root, doc, name="recover-rejected")
    print("PASS configure failure/rejected observation invalidate old provenance and recover explicitly", flush=True)

    # Corrupt, missing and structurally incomplete snapshots fail closed and do
    # not create queries or fall through to an old success.
    index = Path(inspect(build)["indexFile"])
    original = index.read_bytes()
    for content in (b"{", b"{}"):
        index.write_bytes(content)
        before = fingerprint(build)
        assert inspect(build)["state"] == "Unknown"
        rejected(root, adopted, "Unknown")
        assert fingerprint(build) == before
    index.write_bytes(original)
    model_file = next(o["jsonFile"] for o in json.loads(original)["objects"] if o["kind"] == "codemodel")
    model = index.parent / model_file
    model_bytes = model.read_bytes()
    model.unlink()
    rejected(root, adopted, "Unknown")
    model.write_bytes(model_bytes)
    print("PASS corrupt/incomplete/missing File API data causes no mutation or execution", flush=True)

    slow = copy.deepcopy(adopted)
    slow["nodes"] = [first | {"target": "slow", "dependsOn": []}, run]
    manifest = root / "cancel-build.json"
    manifest.write_text(json.dumps(slow))
    results = root / "cancel-build-results"
    with (root / "cancel-build.log").open("w") as stdout:
        process = suite.start_cli([str(CLI), "workflow", "run", str(manifest), "--results-dir", str(results)],
                                  stdout=stdout, stderr=subprocess.STDOUT)
        try:
            # cmake -E touch creates an empty marker; wait_for expects content.
            end = time.monotonic() + 15
            while not (build / "building").exists():
                assert process.poll() is None and time.monotonic() < end
                time.sleep(0.03)
            suite.cancel_cli(process)
            assert process.wait(timeout=10) == 130
        finally:
            suite.kill_cli(process)
    report = json.loads(next(results.glob("*/run.json")).read_text())
    assert report["status"] == "canceled" and nodes(report)["run"]["status"] == "canceled"
    assert not (build / "BUILD_COMPLETED").exists() and not (build / "slow").exists()
    workflow(root, adopted, name="after-build-cancel")
    print("PASS console cancellation during native build: no dependent run or completed output", flush=True)

    # Cancel a configure in a tree with a prior successful managed record.
    rules.write_text(original_rules + '''
file(WRITE "${CMAKE_BINARY_DIR}/configuring" "started")
execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 30)
file(WRITE "${CMAKE_BINARY_DIR}/CONFIGURE_COMPLETED" "done")
''')
    manifest = root / "cancel-configure.json"
    manifest.write_text(json.dumps(doc))
    results = root / "cancel-configure-results"
    with (root / "cancel-configure.log").open("w") as stdout:
        process = suite.start_cli([str(CLI), "workflow", "run", str(manifest), "--results-dir", str(results)],
                                  stdout=stdout, stderr=subprocess.STDOUT)
        try:
            suite.wait_for(build / "configuring", process)
            record = json.loads((build / "dagflow-tools/configure.json").read_text())
            assert record["state"] == "Attempted" and record["indexFile"] is None
            suite.cancel_cli(process)
            assert process.wait(timeout=10) == 130
        finally:
            suite.kill_cli(process)
    assert not (build / "CONFIGURE_COMPLETED").exists()
    assert json.loads((build / "dagflow-tools/configure.json").read_text())["state"] == "Interrupted"
    rejected(root, adopted, "ConfigureFailed")
    rules.write_text(original_rules)
    workflow(root, doc, name="after-configure-cancel")
    print("PASS configure console cancellation invalidates prior success and recovers explicitly", flush=True)


with tempfile.TemporaryDirectory(prefix="dagflow-context-") as temporary:
    main(Path(temporary))
print("All build context lifecycle integration checks passed.", flush=True)
