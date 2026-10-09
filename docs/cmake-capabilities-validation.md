# CMake capability validation

Checked on 2026-10-04 against this DagFlow working tree, on Linux with
CMake 4.4.3, Ninja, Clang 22.1.8 and GCC 16.2.1. This is an integration audit,
not a performance benchmark or a claim that every platform/backend is covered.
Builds, logs and reports from this run are in `/tmp/dagflow-capabilities-check`.

All 16 primitive capabilities and the five compositions (`developer`, `quality`,
`distribution`, `ci`, `full`) configured successfully. Configuration alone is
not sufficient: the executed checks and remaining gaps are listed below.

| Capability | Executed check and result |
| --- | --- |
| `project-minimal` | Standalone configure; embedded `add_subdirectory` build and running consumer. Embedded mode selected `project-minimal` and did not create developer targets. |
| `compile-commands` | Generated and parsed `compile_commands.json` for actual DagFlow targets. |
| `compiler-cache` | Built the runtime through real ccache with its cache confined to `/tmp`. |
| `formatting` | `format-check` runs the correct tool, but fails on existing formatting violations. No mass reformatting was performed. `cmake-format` is not installed. |
| `static-analysis` | clang-tidy and clazy targets completed for the configured runtime sources, with warnings. The initial full-tree clang-tidy invocation exceeded a 180-second limit; a clean full-tree quality gate is not claimed. cppcheck and IWYU are not installed. |
| `testing` | Built shared/static libraries, examples and tests; `boilerplate_check` passed all 31 tests, including CMake regressions. |
| `property-testing` | RapidCheck/GTest property executed against real `Pool`/`TaskScope`; explicit FetchContent downloaded the pinned RapidCheck revision. |
| `fuzzing` | Bounded libFuzzer CTest smoke executed against real `Pool`/`TaskScope`. The fixture uses UBSan for its harness. This is not a long fuzz campaign or validation of AFL++, honggfuzz or FuzzTest. |
| `coverage-report` | Actual tests and HTML generation passed with both Clang and GCC. Clang reports line coverage by default; GCC also reports functions. |
| `docs` | The public API and selected design pages generate successfully with the default warning-as-error setting. Internal implementation details are omitted from the public input set; unresolved links to optional/generated benchmark artifacts are disabled by default with `BOILERPLATE_DOC_WARN_IF_DOC_ERROR=OFF`. |
| `packaging` | TGZ creation, installation, separate `find_package(DagFlow)` consumer build and execution passed. DEB/RPM/Windows/macOS packaging was not exercised. |
| `reproducible-build` | Verified source/debug prefix-map flags on the actual compile commands. Byte-for-byte reproducibility across separate builds was not tested. |
| `build-info` | Parsed the JSON manifest; compiled and ran a consumer of `boilerplate::build_info`, checking project/version/compiler metadata. |
| `diagnostics` | Generated the project summary and ran `dagflow_config`. |
| `cuda` | Configured the capability and compiled/archived a real `.cu` target with nvcc 13.4.92. No GPU kernel execution was performed. |
| `web-deployment` | Native configuration correctly reports that the capability is inactive. Emscripten is not installed, so web artifacts were not built or deployed. |

## Fixes found by the audit

- Independent tool discovery no longer reuses one `_tool` cache entry for
  clang-format, clang-tidy, coverage tools and other executables. Old build
  directories containing that stale entry also work.
- Static analysis selects configured target sources, excluding disabled
  benchmarks and standalone test consumers that lack compile commands.
- `coverage-report` inherits `testing`. Clang uses `llvm-cov gcov`; external
  files are excluded during capture. Unsupported LLVM function-position
  heuristics are disabled, and coverage instrumentation uses atomic counter
  updates to avoid corrupt multithreaded GCC counts.
- CUDA language enablement runs in the calling directory, so compiler/archive
  rules survive configure-hook scope and subsequent CUDA targets can build.
- FetchContent uses a full clone for a pinned commit hash. RapidCheck skips
  unused upstream test submodules; it uses the separately resolved GoogleTest.

The generic capability fixes were also transferred to the separate
`cmake_boilerplate` repository on 2026-10-05. Its regression fixture has no
DagFlow dependency. DagFlow still uses a local copy of the modules; updates are
not synchronized automatically.

Documentation inputs, the API main page and relaxed comment/reference checks
are selected by DagFlow's root CMake configuration. The reusable module keeps
strict defaults and exposes `DOC_WARN_IF_UNDOCUMENTED`, `DOC_WARN_NO_PARAMDOC`,
`DOC_WARN_IF_DOC_ERROR`, and `DOC_MARKDOWN_ID_STYLE` settings (with each
project's usual prefix). A successful DagFlow `docs` build does not establish
complete parameter documentation or validate every Markdown link.

## Repeat the checks

Run from the DagFlow repository root. These commands build and run correctness
checks, not benchmark campaigns.

```sh
cmake -S . -B out/capabilities -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DDAGFLOW_ALLOCATOR=system \
  -DDAGFLOW_BUILD_STATIC=ON -DDAGFLOW_BUILD_TESTS=ON \
  -DDAGFLOW_PROJECT_CAPABILITIES=full -DBOILERPLATE_PACKAGE_GENERATORS=TGZ
cmake --build out/capabilities --target boilerplate_check -j 4
cmake --build out/capabilities --target dagflow_config package

# These enforce the configured quality gates.
cmake --build out/capabilities --target format-check
cmake --build out/capabilities --target docs

# Independent integration fixture: libFuzzer, metadata, optional CUDA/RapidCheck.
cmake -S tests/cmake_capabilities -B out/capability-smoke -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ -DCHECK_CUDA=ON -DCHECK_PROPERTY=ON \
  -DBOILERPLATE_DEPENDENCY_PROVIDER=fetchcontent
cmake --build out/capability-smoke --target boilerplate_check -j 4

# Report generation; use g++ instead to exercise GCC/gcov.
cmake -S . -B out/coverage -G Ninja -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Debug -DDAGFLOW_ALLOCATOR=system \
  -DDAGFLOW_BUILD_EXAMPLES=OFF -DDAGFLOW_BUILD_TESTS=ON \
  -DDAGFLOW_PROJECT_CAPABILITIES=coverage-report -DBOILERPLATE_COVERAGE=ON
cmake --build out/coverage --target coverage-report -j 4
```

The integration fixture requires Clang/libFuzzer. Leave `CHECK_CUDA` and
`CHECK_PROPERTY` off to run without CUDA/RapidCheck. For an existing RapidCheck
checkout, use `BOILERPLATE_RAPIDCHECK_SOURCE_DIR` instead of fetching. CUDA requires
nvcc; `BOILERPLATE_CUDA_ARCHITECTURES` can specify a target when no GPU is present.

Coverage uses lcov/genhtml (tested with lcov 2.x). `BOILERPLATE_COVERAGE_GCOV_TOOL`
overrides the tool as a CMake list, e.g. `/path/to/llvm-cov;gcov`.
`BOILERPLATE_COVERAGE_EXCLUDES` controls source exclusions.
`BOILERPLATE_COVERAGE_FUNCTIONS` defaults to OFF for Clang and ON for GCC because
LLVM's gcov compatibility output can lack usable function positions for genhtml.
No coverage-data errors are ignored to make the checks pass; unmatched exclusion
patterns are allowed because optional source directories may be absent.
