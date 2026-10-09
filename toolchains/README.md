# DagFlow cross compilation

SDK selection belongs to CMake toolchains, not to target policies. These files
are imported from the dagflow toolchain layer. No SDK is downloaded and
none of the presets activates vcpkg implicitly.

| Preset | Compiler/SDK | Validation |
| --- | --- | --- |
| `native-linux-clang` | Native Linux Clang | Native compile, tests and install |
| `linux-arm64` | Debian/Ubuntu `g++-aarch64-linux-gnu` | Cross build, AArch64 ELF and package consumer in CI |
| `windows-x64-from-linux` | LLVM-MinGW (`LLVM_MINGW_ROOT`) | Opt-in, SDK required |
| `android-arm64` | Android NDK (`ANDROID_NDK_ROOT` or `ANDROID_NDK_HOME`) | Opt-in, SDK required |
| `wasm32-emscripten` | Emscripten (`EMSDK`) | Experimental: DagFlow requires a compatible threaded runtime and toolchain; not covered by CI |

```sh
cmake --preset linux-arm64
cmake --build --preset linux-arm64 --parallel 2
cmake --install out/build/linux-arm64 --prefix /tmp/dagflow-aarch64
```

Cross builds compile/install only by default. Executing target binaries requires
an explicit `CMAKE_CROSSCOMPILING_EMULATOR` plus a usable target libc/runtime;
not every DagFlow test helper forwards emulator arguments. The AArch64 CI job
checks both installed library and an installed-package consumer without trying
to execute foreign binaries.

For an SDK + vcpkg supply the matching **target** triplet and use the vcpkg
toolchain with `VCPKG_CHAINLOAD_TOOLCHAIN_FILE=<toolchain in this directory>`.
Do not use host-library dependencies when cross compiling. Avoid native CPU
flags (`DAGFLOW_ENABLE_NATIVE`) for any cross target.
