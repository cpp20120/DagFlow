#!/bin/sh
set -eu
printf 'mock tool called:'
printf ' [%s]' "$@"
printf '\n'
# For the fixture only: mimic bridge --prepared by printing the executable path.
# The C# code is the real implementation delivered in tools/DagFlow.Tools.
