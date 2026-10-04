"""Project import bridge to the vendored, dependency-free process harness."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cmake" / "harness"))
from dagflow_harness import (CommandRunner, digest, physical_cpus, save_json,
                                parse_perf_stat, tree_digests)
