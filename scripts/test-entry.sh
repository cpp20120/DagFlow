#!/usr/bin/env bash
# Offline checks: no package installations, downloads, or source-tree builds.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bash "$root/setup.sh" --dry-run --manager apt --vcpkg-root "$work/no-sdk" --run > "$work/plan"
grep -q -- '-DPRESETS=core' "$work/plan"
grep -q -- '-DRUN_APPLICATION=ON' "$work/plan"
test ! -e "$work/no-sdk"
bash "$root/build.sh" --dry-run --preset bench-check --jobs 3 --run-target dagflow_run_runtime_suite > "$work/bench"
grep -q -- '-DPRESETS=bench-check' "$work/bench"
grep -q -- '-DRUN_TARGET=dagflow_run_runtime_suite' "$work/bench"
bash "$root/build.sh" --dry-run --package-format TGZ --no-tests > "$work/package"
grep -q -- 'PACKAGING_OPTION=DAGFLOW_ENABLE_PACKAGING' "$work/package"
grep -q -- 'DAGFLOW_INSTALL=ON' "$work/package"
grep -q -- '-DRUN_TESTS=OFF' "$work/package"
for invalid in '--init Demo' '--output x' '--jobs -5' '--preset ../../wrong' '--run-target bad/target' '--package-format BAD'; do
  if bash "$root/setup.sh" --dry-run $invalid >/dev/null 2>&1; then
    echo "Accepted invalid arguments: $invalid"; exit 1
  fi
done

# Test activation and propagation across actual setup/build entry points using
# a fake host installer and CMake command. Include quotes/spaces in every path.
fixture="$work/project ' space"
mkdir -p "$fixture/scripts" "$fixture/bin" "$fixture/cmake/dagflow/build"
cp "$root/setup.sh" "$root/build.sh" "$fixture/"
cp "$root/scripts/entry-options.sh" "$root/scripts/default-preset.txt" "$fixture/scripts/"
export ENTRY_TEST_ROOT="$fixture" ENTRY_TEST_LOG="$work/commands"
cat > "$fixture/setup-host.sh" <<'MOCK'
#!/usr/bin/env bash
[[ "${ENTRY_FAIL_HOST:-0}" == 0 ]] || exit "$ENTRY_FAIL_HOST"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == --tools-dir ]]; then tools="$2"; shift; fi
  shift
done
mkdir -p "$tools"
if [[ -f "$tools/ready" ]]; then echo ALREADY; else echo INSTALLED; touch "$tools/ready"; fi
printf 'export PATH=%q:"$PATH"\nexport ENTRY_ACTIVE=yes\n' "$ENTRY_TEST_ROOT/bin" > "$tools/env.sh"
MOCK
cat > "$fixture/bin/cmake" <<'MOCK'
#!/usr/bin/env bash
[[ "${ENTRY_ACTIVE:-}" == yes ]] || exit 91
printf '%s\n' "$@" >> "$ENTRY_TEST_LOG"
exit "${ENTRY_FAIL_BUILD:-0}"
MOCK
chmod +x "$fixture/bin/cmake"
for phase in first second; do
  bash "$fixture/setup.sh" --tools-dir "$fixture/tools ' space" --run > "$work/$phase"
done
grep -q INSTALLED "$work/first"
grep -q ALREADY "$work/second"
grep -q -- '-DRUN_APPLICATION=ON' "$ENTRY_TEST_LOG"
[[ "$(grep -c -- '-DPRESETS=core' "$ENTRY_TEST_LOG")" == 2 ]]
rm "$ENTRY_TEST_LOG"
bash "$fixture/setup.sh" --setup-only --tools-dir "$fixture/tools ' space" > /dev/null
test ! -e "$ENTRY_TEST_LOG"
if ENTRY_FAIL_HOST=27 bash "$fixture/setup.sh" > /dev/null; then exit 1; else [[ $? == 27 ]]; fi
test ! -e "$ENTRY_TEST_LOG"
if ENTRY_FAIL_BUILD=31 bash "$fixture/setup.sh" > /dev/null; then exit 1; else [[ $? == 31 ]]; fi
[[ "${ENTRY_ACTIVE:-}" != yes ]]
echo 'DagFlow shell entry: activation, reuse, argument mapping and failure propagation passed.'
