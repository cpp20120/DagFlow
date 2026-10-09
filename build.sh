#!/usr/bin/env bash
set -eo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
entry=build.sh
source "$root/scripts/entry-options.sh"
[[ "$setup_only" == false ]] || { echo 'Use setup.sh for --setup-only.' >&2; exit 2; }
if [[ "$dry_run" == false && -f "$tools_dir/env.sh" ]]; then
  source "$tools_dir/env.sh"
fi
module="$root/lib/cmake/build/BuildMatrix.cmake"
[[ -f "$module" ]] || module="$root/cmake/boilerplate/build/BuildMatrix.cmake"
project_options=()
# Adapt the generic matrix's install/package switches to DagFlow's own options.
if [[ "$package_artifacts" == true ]]; then
  project_options+=("-DCONFIGURE_ARGS=-DDAGFLOW_ENABLE_PACKAGING=ON;-DDAGFLOW_INSTALL=ON")
elif [[ "$install_artifacts" == true ]]; then
  project_options+=("-DCONFIGURE_ARGS=-DDAGFLOW_INSTALL=ON")
fi
command_args=(cmake "-DSOURCE_DIR=$root" "-DPRESETS=$preset" "-DJOBS=$jobs"
  "-DINSTALL_ARTIFACTS=$([[ "$install_artifacts" == true ]] && echo ON || echo OFF)"
  "-DRUN_TESTS=$([[ "$run_tests" == true ]] && echo ON || echo OFF)"
  "-DRUN_APPLICATION=$([[ "$run_application" == true ]] && echo ON || echo OFF)"
  "-DPACKAGE_ARTIFACTS=$([[ "$package_artifacts" == true ]] && echo ON || echo OFF)"
  "-DPACKAGE_FORMAT=$package_format"
  "${project_options[@]}" -DBOILERPLATE_VCPKG_BOOTSTRAP=ON "-DRUN_TARGET=$run_target" -P "$module")
if [[ "$dry_run" == true ]]; then
  printf '+'; printf ' %q' "${command_args[@]}"; printf '\n'
  exit 0
fi
command -v cmake >/dev/null 2>&1 || { echo 'CMake missing: run ./setup.sh first.' >&2; exit 1; }
exec "${command_args[@]}"
