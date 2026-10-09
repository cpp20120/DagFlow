# DagFlow CMake integration

`cmake/dagflow/` contains the build infrastructure maintained with DagFlow.
`cmake/DagFlow*.cmake` describes runtime sources, optional allocators, tests,
benchmarks and campaigns. Settings use `DAGFLOW_*`; functions and workflow
targets use `dagflow_*`. The main entry module is `cmake/dagflow/DagFlow.cmake`.

The code originated in the independent cmake_boilerplate project; its source
revisions and license are recorded in [UPSTREAM.md](../cmake/dagflow/UPSTREAM.md).
That project retains its own release lifecycle. DagFlow neither needs a sibling
checkout nor changes it when its own build infrastructure is updated.

The retained modules provide host setup, dependency discovery and pinned vcpkg
bootstrap, compiler/linker probes, shared/static library construction, profiles,
CTest, fuzz/property checks, coverage, benchmark harness, PGO, install/export,
packaging, formatting, analysis and documentation. Application deployment,
GPU/UI/shader policies and template example presets have been removed.

`setup.sh` / `setup.ps1` prepare tools, then call `build.sh` / `build.ps1`.
The default preset is `core`; `dagflow_run` executes the basic example. Benchmarks
use their `dagflow_run_*` targets and campaigns use the process harness.

After the namespace change, use a fresh build directory or `cmake --fresh
--preset <name>` to avoid old cached options. Existing commands must use
`DAGFLOW_*` settings and the new target names; there are no legacy aliases.

Run the host/entry script tests, the `core` and benchmark CTest presets, and
install/embedded consumer checks after changes. CI exercises Linux, macOS and
Windows; its configured matrix is not a claim that every platform was tested
locally.
