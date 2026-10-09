# CUDA target profiles

The `cuda` project capability enables the language before targets are created.
The `cuda` target policy configures individual targets. Ordinary projects do not
discover or require the CUDA Toolkit unless they enable that capability.

```cmake
include(cmake/Boilerplate.cmake)
boilerplate_project(TOP_LEVEL_CAPABILITIES developer cuda)

boilerplate_add_library(gpu_compute
  SOURCES src/kernels.cu src/device_functions.cu src/host.cpp
  INCLUDE_DIR include
  POLICIES runtime-cuda cuda-profiled)

boilerplate_add_executable(compute_demo
  SOURCES src/main.cpp
  LIBRARIES gpu_compute::gpu_compute
  POLICIES runtime-cuda)
boilerplate_finalize_project()
```

`runtime-cuda` composes `runtime` and `cuda`, resetting CPU tuning through the
runtime's `minimal` baseline. The plain `cuda` policy preserves existing target
settings and validates compatibility. Both use NVIDIA nvcc; Clang's CUDA driver
is not currently supported by these policies.

| Policy | Effect |
| --- | --- |
| `cuda` | CUDA standard/runtime properties and separate device compilation |
| `runtime-cuda` | `runtime` + `cuda` |
| `cuda-debug` | CUDA device debugging (`-G`) and debug symbols |
| `cuda-profiled` | CUDA line information without enabling device debugging |
| `cuda-fast-math` | Explicit opt-in to nvcc's fast math semantics |

Debug and profiled policies override each other's device debug/line-info choices.
Fast math is OFF by default and can change numerical results. nvcc's device debug
mode disables device optimizations by default; use line information for profiling.
See the [NVIDIA compiler options](https://docs.nvidia.com/cuda/cuda-compiler-driver-nvcc/).

Defaults:

- `BOILERPLATE_CUDA_STANDARD=20` (supported: 17, 20; independent of C++23 host code).
- `BOILERPLATE_CUDA_ARCHITECTURES=native` (use explicit architectures for CI/distribution).
- `BOILERPLATE_CUDA_SEPARABLE_COMPILATION=ON` (cross-file device calls/device linking).
- `BOILERPLATE_CUDA_RUNTIME_LIBRARY=Static` (`Static`, `Shared`, `None`).

Per-target settings use the same `POLICY_OPTIONS` mechanism as CPU policies:

```cmake
boilerplate_add_executable(gpu_tool SOURCES main.cu
  POLICIES runtime-cuda
  POLICY_OPTIONS CUDA_STANDARD 17 CUDA_RUNTIME_LIBRARY Shared CUDA_LINEINFO ON)
set_property(TARGET gpu_tool PROPERTY CUDA_ARCHITECTURES "75;86")
```

Other target keys are `CUDA`, `CUDA_SEPARABLE_COMPILATION`, `CUDA_DEVICE_DEBUG`
and `CUDA_FAST_MATH`. `minimal` resets CUDA enablement and device flags. Architecture
lists use the native [CMake CUDA_ARCHITECTURES property](https://cmake.org/cmake/help/latest/prop_tgt/CUDA_ARCHITECTURES.html).
`None` runtime linkage is an advanced choice: the caller must supply required runtime
symbols. CUDA toolkit/host-compiler/architecture compatibility still applies.

Select a toolkit or supported host compiler on the initial configure with
`CMAKE_CUDA_COMPILER` / `CMAKE_CUDA_HOST_COMPILER`. Without a GPU/driver, avoid
`native` and pass e.g. `-DBOILERPLATE_CUDA_ARCHITECTURES=75`. The example architecture
is a build-check baseline, not a recommendation for all applications.

C++ warning flags are scoped to C++ compilation; CUDA-specific flags are scoped
to CUDA. CPU LTO/PGO, sanitizers, CFI/CFG, hardening and CPU-specific tuning on a CUDA
target fail with a configure-time explanation. Keep such policies on separate CPU
targets; they are not CUDA device instrumentation. This restriction does not make
arbitrary instrumented CPU dependencies compatible with nvcc's linker automatically.

## Validation

From the repository root, `app-cuda-debug` and `app-cuda-release` enable
`developer;cuda` capabilities and select Debug/Release builds. Matching configure,
build, test and workflow presets are available. They default to `native`
architectures and do not apply target policies automatically: use `runtime-cuda`
on GPU targets and add `cuda-debug` for device debugging or `cuda-profiled` for
line information. The sample application itself remains CPU-only.

```sh
cmake --workflow --preset app-cuda-release
# Without a working GPU/driver, configure an explicit architecture instead:
cmake --preset app-cuda-debug -DBOILERPLATE_CUDA_ARCHITECTURES=75
cmake --build --preset app-cuda-debug
ctest --preset app-cuda-debug
cmake --build --preset app-cuda-debug --target boilerplate_check_cuda
```

The CUDA fixtures can also be checked from an ordinary CPU configuration:

```sh
cmake --preset app-debug
cmake --build --preset app-debug --target boilerplate_check_cuda
# Standalone, with an optional explicit host compiler:
cmake -DCHECK_BINARY=/tmp/cuda-check -DCHECK_CUDA_HOST_COMPILER=/path/to/g++ \
  -P lib/tests/cmake/verify_cuda.cmake
```

The check uses real nvcc, shared/static libraries containing both C++ and CUDA,
cross-file device calls, executable device linking, static/shared CUDA runtime
selection and all three device flag profiles. Host-only smoke executables run
without launching a kernel. It also checks rejection of missing language setup,
CPU LTO/sanitizers and unsupported CUDA standards. `CHECK_GENERATOR=Ninja Multi-Config`
selects the multi-config check. GPU execution and numerical correctness require
application-specific tests on a machine with a working NVIDIA driver/GPU.
