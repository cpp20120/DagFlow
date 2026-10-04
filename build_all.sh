#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec cmake "-DSOURCE_DIR=${root}" "$@" -P "${root}/cmake/BuildDagFlow.cmake"
