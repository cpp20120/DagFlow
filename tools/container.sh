#!/usr/bin/env bash
set -euo pipefail
usage() { echo 'Usage: tools/container.sh dev|build|test|package|shell [--preset NAME] [--image NAME]'; }
action=${1:-}; [[ $# -gt 0 ]] && shift
case "$action" in dev|build|test|package|shell) ;; *) usage >&2; exit 2;; esac
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
preset=core
image=dagflow-sdk
while [[ $# -gt 0 ]]; do
  case "$1" in
    --preset) preset=${2:?}; shift 2;;
    --image) image=${2:?}; shift 2;;
    *) usage >&2; exit 2;;
  esac
done
engine=$(command -v docker || command -v podman || true)
[[ -n "$engine" ]] || { echo 'Docker or Podman is required' >&2; exit 1; }
if [[ "$action" == shell ]]; then
  "$engine" build --target dev -t "$image:dev" "$root"
  exec "$engine" run --rm -it -v "$root:/workspace" -w /workspace "$image:dev" bash
fi
exec "$engine" build --target "$action" --build-arg "CMAKE_PRESET=$preset" -t "$image:$action" "$root"
