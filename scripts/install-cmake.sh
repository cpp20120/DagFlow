# Called only if the distribution's CMake remains older than 3.26.
install_local_cmake() {
  local version=3.31.8 arch archive base staging expected actual
  arch="$(uname -m)"
  case "$arch" in x86_64|aarch64) ;; *) echo "No bundled CMake for Linux/$arch" >&2; return 1;; esac
  archive="cmake-$version-linux-$arch.tar.gz"
  base="https://github.com/Kitware/CMake/releases/download/v$version"
  mkdir -p "$tools_dir"
  staging="$(mktemp -d "$tools_dir/cmake-download.XXXXXX")" || return 1
  if ! run curl --fail --location --retry 3 "$base/$archive" -o "$staging/$archive" ||
     ! run curl --fail --location --retry 3 "$base/cmake-$version-SHA-256.txt" -o "$staging/checksums"; then
    rm -rf "$staging"; return 1
  fi
  expected="$(awk -v name="$archive" '$2 == name {print $1}' "$staging/checksums")"
  actual="$(sha256sum "$staging/$archive")"; actual="${actual%% *}"
  if [[ ! "$expected" =~ ^[0-9a-fA-F]{64}$ || "$actual" != "$expected" ]]; then
    echo 'CMake archive SHA256 mismatch' >&2; rm -rf "$staging"; return 1
  fi
  if ! tar -xzf "$staging/$archive" -C "$staging"; then rm -rf "$staging"; return 1; fi
  if [[ ! -x "$staging/cmake-$version-linux-$arch/bin/cmake" ]]; then
    rm -rf "$staging"; return 1
  fi
  rm -rf "$tools_dir/cmake"
  mv "$staging/cmake-$version-linux-$arch" "$tools_dir/cmake" || return 1
  rm -rf "$staging"
  hash -r
}
