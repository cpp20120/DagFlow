"""Reusable process benchmark/stress harness primitives."""

from .core import (
    Case,
    CommandRunner,
    digest,
    load_cases,
    parse_cpu_list,
    parse_perf_stat,
    physical_cpus,
    save_json,
)

__all__ = [
    "Case",
    "CommandRunner",
    "digest",
    "load_cases",
    "parse_cpu_list",
    "parse_perf_stat",
    "physical_cpus",
    "save_json",
    "bootstrap_median_ci",
    "paired_summary",
    "randomized_paired_jobs",
    "tree_digests",
]

from .experiment import bootstrap_median_ci, paired_summary, randomized_paired_jobs, tree_digests
