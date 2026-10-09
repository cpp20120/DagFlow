#!/usr/bin/env bash
# Offline checks with a deliberately minimal PATH and no Python executable.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin"
for tool in bash dirname basename uname; do ln -s "$(command -v "$tool")" "$work/bin/$tool"; done
# Any attempt to execute a package manager in dry-run is a test failure.
for tool in apt-get dnf pacman brew sudo git; do
  cat > "$work/bin/$tool" <<'MOCK'
#!/bin/bash
if [[ "$1" == -Q ]]; then exit 1; fi
printf 'unexpected installer execution\n' >> "$SETUP_TEST_MARKER"
exit 90
MOCK
  chmod +x "$work/bin/$tool"
done
# brew --prefix is a read-only query made by the script.
cat > "$work/bin/brew" <<'MOCK'
#!/bin/bash
if [[ "$1" == --prefix ]]; then echo /not-installed-brew; exit 0; fi
if [[ "$1" == list ]]; then exit 1; fi
printf 'unexpected brew install\n' >> "$SETUP_TEST_MARKER"
exit 90
MOCK
export SETUP_TEST_MARKER="$work/mutated"
for manager in apt dnf pacman brew; do
  PATH="$work/bin" /bin/bash "$root/setup-host.sh" --dry-run --manager "$manager" \
    --tools-dir "$work/tools with spaces" --vcpkg-root "$work/new vcpkg" > "$work/$manager.log"
  test ! -e "$work/mutated"
  test ! -e "$work/tools with spaces"
  test ! -e "$work/new vcpkg"
  for expected in cmake ninja sccache ccache vcpkg.git; do grep -qi "$expected" "$work/$manager.log"; done
  if grep '^+' "$work/$manager.log" | grep -Ei 'pip|venv|python'; then echo 'Unexpected Python install command'; exit 1; fi
done
for arguments in '--check --install' '--with-emscripten' '--tools-dir' '--check --profile unknown' '--check-harness'; do
  # Intentional splitting: these are fixed invalid argument lists.
  if PATH="$work/bin" /bin/bash "$root/setup-host.sh" $arguments > /dev/null 2>&1; then exit 1; fi
done
if PATH="$work/bin" /bin/bash "$root/setup-host.sh" --dry-run --vcpkg-root "$root/vcpkg/../vcpkg" > /dev/null 2>&1; then exit 1; fi
# --check must not run a package manager or invoke Python either.
if PATH="$work/bin" /bin/bash "$root/setup-host.sh" --check --manager apt --vcpkg-root "$work/new vcpkg" > "$work/check.log"; then exit 1; fi
grep -q 'MISSING.*clangd' "$work/check.log"
test ! -e "$work/mutated"
for profile in minimal dev ci; do
  PATH="$work/bin" /bin/bash "$root/setup-host.sh" --dry-run --manager apt --profile "$profile" \
    --vcpkg-root "$work/new vcpkg" > "$work/profile-$profile.log"
  grep -q 'Summary:' "$work/profile-$profile.log"
done
! grep -q 'clangd' "$work/profile-minimal.log"
! grep -q 'clangd' "$work/profile-ci.log"
grep -q 'clangd' "$work/profile-dev.log"
grep -q 'clang-tidy' "$work/profile-ci.log"
# Exercise environment writing and propagation of failed installation/checks.
# Existing vcpkg is reused and never pulled/reset.
mkdir -p "$work/sdk ' literal/scripts/buildsystems"
touch "$work/sdk ' literal/scripts/buildsystems/vcpkg.cmake"
printf '#!/bin/bash\nexit 0\n' > "$work/sdk ' literal/vcpkg"
chmod +x "$work/sdk ' literal/vcpkg"
for tool in apt-get sudo; do
  printf '#!/bin/bash\nexit 0\n' > "$work/bin/$tool"
done
ln -s "$(command -v mkdir)" "$work/bin/mkdir"
if PATH="$work/bin" /bin/bash "$root/setup-host.sh" --install --manager apt \
  --vcpkg-root "$work/sdk ' literal" --tools-dir "$work/env ' literal" > "$work/install.log" 2>&1; then
  echo 'Incomplete installation must fail'; exit 1
fi
# Only run the mock install on Linux/apt hosts; other platforms reject the manager.
if [[ -f "$work/env ' literal/env.sh" ]]; then
  observed="$(/bin/bash -c 'source "$1"; printf "%s" "$VCPKG_ROOT"' check "$work/env ' literal/env.sh")"
  test "$observed" = "$work/sdk ' literal"
  grep -q 'Setup incomplete' "$work/install.log"
fi
# An old CMake is upgraded; a second run must not invoke any installer.
if [[ "$(uname -s)" == Linux ]]; then
  while IFS='|' read -r label command_name rest; do
    [[ "$label" == \#* || "$command_name" == @package ]] && continue
    [[ "$command_name" == git ]] && continue
    printf '#!/bin/bash\nexit 0\n' > "$work/bin/$command_name"
    chmod +x "$work/bin/$command_name"
  done < "$root/scripts/host-tools.txt"
  printf '#!/bin/bash\necho "install ok installed"\n' > "$work/bin/dpkg-query"
  printf '#!/bin/bash\necho "cmake version 3.25.0"\n' > "$work/bin/cmake"
  chmod +x "$work/bin/dpkg-query"
  export SETUP_TEST_BIN="$work/bin"
  printf '#!/bin/bash\nexec "$@"\n' > "$work/bin/sudo"
  cat > "$work/bin/apt-get" <<'MOCK'
#!/bin/bash
echo "$*" >> "$SETUP_TEST_MARKER"
if [[ "$1" == install ]]; then
  printf '#!/bin/bash\necho "cmake version 3.26.0"\n' > "$SETUP_TEST_BIN/cmake"
fi
MOCK
  PATH="$work/bin" /bin/bash "$root/setup-host.sh" --install --profile minimal \
    --vcpkg-root "$work/sdk ' literal" --tools-dir "$work/idempotent-env" > "$work/first.log"
  grep -q 'INSTALLED.*CMake' "$work/first.log"
  test -f "$work/mutated"
  rm "$work/mutated"
  PATH="$work/bin" /bin/bash "$root/setup-host.sh" --install --profile minimal \
    --vcpkg-root "$work/sdk ' literal" --tools-dir "$work/idempotent-env" > "$work/second.log"
  grep -q 'installed=0 missing=0' "$work/second.log"
  test ! -e "$work/mutated"
fi
echo 'Native shell setup checks passed.'
