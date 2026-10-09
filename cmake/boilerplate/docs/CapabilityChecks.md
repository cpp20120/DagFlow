# Capability fixes and validation

Project capabilities work with any C++ application or library. The following
fixes were found while integrating the framework into a multithreaded runtime:

- Tool discovery is independent for each executable, even with stale CMake caches.
- Static analysis uses configured target sources instead of disabled examples
  and standalone consumers that have no compile commands in the current build.
- `coverage-report` inherits `testing`. Coverage counters update atomically;
  Clang selects `llvm-cov gcov` and GCC uses gcov.
- CUDA compiler rules survive configure hooks because the project entry macro
  enables languages in directory scope.
- FetchContent supports pinned commit hashes using full clones. `NO_SUBMODULES`
  skips optional submodules for that provider; RapidCheck uses this because its
  upstream tests are disabled and GoogleTest is resolved separately.

No runtime, allocator, project name, benchmark workload or local source path is
required by these fixes.

## Documentation settings

`BOILERPLATE_DOC_INPUTS`, `BOILERPLATE_DOC_MAINPAGE`,
`BOILERPLATE_DOC_EXCLUDE_PATTERNS`, and `BOILERPLATE_DOC_EXCLUDE_SYMBOLS` control
the documentation scope. `BOILERPLATE_DOC_EXTRACT_STATIC` defaults to OFF for
file-local implementation details.
`BOILERPLATE_DOC_MARKDOWN_ID_STYLE` defaults to `DOXYGEN`; projects using GitHub
heading anchors can explicitly choose `GITHUB`.

The generic defaults remain strict: `BOILERPLATE_DOC_WARN_AS_ERROR`,
`BOILERPLATE_DOC_WARN_IF_DOC_ERROR`, `BOILERPLATE_DOC_WARN_IF_UNDOCUMENTED`, and
`BOILERPLATE_DOC_WARN_NO_PARAMDOC` all default to ON. A consumer can explicitly
disable individual checks without changing the reusable module. Doxygen boolean
values are translated to YES/NO and warning-as-error uses FAIL_ON_WARNINGS, so
all diagnostics are printed before the build fails.

## Coverage settings

`BOILERPLATE_COVERAGE_GCOV_TOOL` overrides the gcov command as a CMake list, e.g.
`/path/to/llvm-cov;gcov`. `BOILERPLATE_COVERAGE_EXCLUDES` selects excluded sources.
`BOILERPLATE_COVERAGE_FUNCTIONS` defaults to OFF on Clang because its gcov
compatibility output may omit usable function positions; GCC defaults to ON.
Report generation was checked with lcov 2.x. Missing optional exclusion matches
are allowed; coverage data errors are not suppressed.

## Run the regression checks

From the repository root (CMake and Ninja required):

```sh
cmake -DCHECK_BINARY=/tmp/boilerplate-capability-check \
  -P lib/tests/cmake/verify_capabilities.cmake
```

Add `-DCHECK_DOCS=ON` to test both successful Doxygen generation and rejection of
an unresolved reference; Doxygen must be installed. Add `-DCHECK_CUDA=ON` for a
real CUDA compilation/archive check (nvcc required, no kernel execution).
Add `-DCHECK_COVERAGE=ON` to generate an HTML report (lcov/genhtml required).
`-DCHECK_COMPILER=/path/to/clang++` or `g++` selects the C++ compiler. Use a fresh
`CHECK_BINARY` directory when changing compilers. These are correctness checks,
not benchmark campaigns.
