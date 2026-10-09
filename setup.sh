#!/usr/bin/env bash
set -eo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
entry=setup.sh
source "$root/scripts/entry-options.sh"
mode=--install
[[ "$dry_run" == false ]] || mode=--dry-run
bash "$root/setup-host.sh" "$mode" --profile "$profile" --tools-dir "$tools_dir" \
  --defer-vcpkg "${host_extra[@]}"
if [[ "$setup_only" == false ]]; then
  exec bash "$root/build.sh" "${build_args[@]}"
fi
