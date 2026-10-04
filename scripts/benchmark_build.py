"""CMake invocation plumbing; targets and optimization policies live in cmake/."""
from pathlib import Path
import re


def source_options(argv):
    """Keep archived experiments buildable against their original CMake API."""
    argv = list(map(str, argv))
    if not argv or argv[0] != "cmake" or "-S" not in argv:
        return argv
    source = Path(argv[argv.index("-S") + 1]) / "CMakeLists.txt"
    content = source.read_text()
    if "cmake/Bootstrap.cmake" not in content:
        argv = [re.sub(r"^-DDAGFLOW_(?=[A-Z_]+=)", "-DDAGFLOW_", arg) for arg in argv]
    if re.search(r"option\(\s*TP_BUILD_SHARED\b", content):
        # Current callers always use DAGFLOW_ names. Translate only for frozen
        # source trees whose CMake still declares the historical options.
        return [re.sub(r"^-DDAGFLOW_(BUILD_[A-Z_]+|INSTALL)(?==)", r"-DTP_\1", arg)
                for arg in argv]
    return argv


def commands(repo: Path, build: Path, compiler: str, profile: str,
             targets: list[str], definitions: dict[str, str]) -> list[list[str]]:
    options = dict(DAGFLOW_BUILD_SHARED="OFF", DAGFLOW_BUILD_STATIC="OFF", DAGFLOW_BUILD_EXAMPLES="OFF",
                   DAGFLOW_BUILD_BENCH="OFF", DAGFLOW_BUILD_RUNTIME_BENCH="OFF",
                   DAGFLOW_BUILD_RUNTIME_SUITE="OFF", DAGFLOW_BUILD_PUBLIC_API_BENCH="OFF",
                   DAGFLOW_BUILD_STRESS_BENCH="OFF", DAGFLOW_BUILD_GITHUB_BENCH="OFF",
                   DAGFLOW_BUILD_FUNCTION_BENCH="OFF", DAGFLOW_BUILD_TESTS="OFF", DAGFLOW_INSTALL="OFF",
                   DAGFLOW_ALLOCATOR="system", DAGFLOW_PROFILE=profile)
    options.update(definitions)
    return [
        ["cmake", "-S", str(repo), "-B", str(build), "-G", "Ninja",
         f"-DCMAKE_CXX_COMPILER={compiler}", *[f"-D{k}={v}" for k, v in options.items()]],
        ["cmake", "--build", str(build), "--target", *targets, "--parallel", "4"],
    ]
