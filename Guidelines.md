# Repository Guidelines

## Scope & Project Structure

DagFlow is a C++23 task runtime and an independent Git repository. Run commands from this directory. Public APIs live in `include/dagflow/`, internal headers in `include/dagflow/detail/`, and implementations in `src/`. Tests, fuzzers, benchmarks, examples, and documentation live in their corresponding top-level directories.

`cmake/DagFlow*.cmake` adapts the runtime to the DagFlow-owned infrastructure in `cmake/dagflow/`. Read `cmake/dagflow/UPSTREAM.md` before porting infrastructure changes; the sibling boilerplate project is maintained separately.

## Build, Test, and Development Commands

Use CMake 3.26+ and a C++23 compiler:

```sh
cmake --preset core
cmake --build --preset core --parallel 2
ctest --preset core --output-on-failure --no-tests=error
```

The core build uses the system allocator without external allocator dependencies. `./build.sh --run` builds, tests, and runs the basic example with existing tools; `./setup.sh --run` also provisions missing prerequisites. PowerShell equivalents are available.

For concurrency changes, consider `check-asan`, `check-tsan`, and `adversarial-tiny`. Configure, build, and test each selected preset separately. `fuzz` builds instrumented harnesses; CTest performs seed replay. Keep compiler and sanitizer configurations in separate build trees.

## Coding Style & Runtime Contracts

Follow `.clang-format`: Google-based C++ style, two spaces, no tabs, 80 columns. Use existing snake_case filenames and functions, public type conventions, and `DAGFLOW_*` CMake settings.

Preserve the separation between storage ownership, packet custody, completion credits, and observing handles. Consult `docs/explicit-ownership.md`, `docs/pool-lifecycle.md`, and the relevant subsystem documentation before changing publication, cleanup, waiting, or shutdown behavior.

## Testing Guidelines

Tests are standalone C++ executables registered with CTest in `cmake/DagFlowTests.cmake`. Use `tests/support.hpp` checks, which remain active in Release, and existing `*_tests.cpp` or `*_test.cpp` names. Rebuild before running CTest. Add bounded regressions for changed behavior; inspect `tests/README.md` for labels and sanitizer exclusions. Use benchmarks separately to support performance claims.

## Commit & Pull Request Guidelines

Use concise imperative subjects; history includes `build:`, `style:`, and `chore:` prefixes. Describe the behavior change, affected platforms/API contracts, and exact validation performed. Preserve unrelated working-tree changes and keep generated builds/profiles out of source changes.

## Project Skill

Use [the DagFlow skill](.agents/skills/dagflow/SKILL.md) for runtime, API, build, fuzzing, or benchmark work in this repository.
