from __future__ import annotations

import random
import statistics
from pathlib import Path
from typing import Any, Iterable, Sequence

from .core import digest, lookup_field


def tree_digests(root: Path) -> dict[str, str]:
    """Hash a source snapshot deterministically, keyed by relative POSIX paths."""
    root = root.resolve()
    return {
        path.relative_to(root).as_posix(): digest(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def randomized_paired_jobs(
    point_count: int,
    variants: Sequence[str],
    rounds: int,
    seed: int,
) -> list[tuple[int, int, tuple[str, ...]]]:
    """Return randomized point order and randomized within-pair variant order."""
    if point_count < 1 or rounds < 1 or len(variants) < 2:
        raise ValueError("paired jobs need points, positive rounds and >=2 variants")
    rng = random.Random(seed)
    jobs = [(trial, point) for trial in range(rounds) for point in range(point_count)]
    rng.shuffle(jobs)
    result: list[tuple[int, int, tuple[str, ...]]] = []
    for trial, point in jobs:
        order = list(variants)
        rng.shuffle(order)
        result.append((trial, point, tuple(order)))
    return result


def bootstrap_median_ci(
    samples: Sequence[float],
    *,
    confidence: float = 0.95,
    resamples: int = 10_000,
    seed: int = 0xB0057,
) -> tuple[float, float]:
    if not samples or resamples < 1 or not 0.0 < confidence < 1.0:
        raise ValueError("invalid bootstrap arguments")
    rng = random.Random(seed)
    values = sorted(
        statistics.median(rng.choices(samples, k=len(samples))) for _ in range(resamples)
    )
    tail = (1.0 - confidence) / 2.0
    low_index = max(0, min(resamples - 1, int(tail * resamples)))
    high_index = max(0, min(resamples - 1, int((1.0 - tail) * resamples) - 1))
    return values[low_index], values[high_index]


def paired_summary(
    rows: Iterable[dict[str, Any]],
    *,
    metric: str,
    baseline: str,
    candidate: str,
    variant_field: str = "variant",
    trial_field: str = "trial",
) -> dict[str, Any]:
    """Summarize one already-grouped paired A/B experiment."""
    rows = list(rows)
    samples: dict[str, dict[int, float]] = {baseline: {}, candidate: {}}
    for row in rows:
        variant = str(row[variant_field])
        if variant not in samples:
            continue
        trial = int(row[trial_field])
        result = row.get("result", row)
        samples[variant][trial] = float(lookup_field(result, metric))
    trials = sorted(set(samples[baseline]) & set(samples[candidate]))
    if not trials:
        raise ValueError("no complete baseline/candidate pairs")
    if set(samples[baseline]) != set(samples[candidate]):
        raise ValueError("baseline/candidate trials are not paired exactly")
    baseline_values = [samples[baseline][trial] for trial in trials]
    candidate_values = [samples[candidate][trial] for trial in trials]
    paired_ratios = [samples[candidate][trial] / samples[baseline][trial] for trial in trials]
    before = statistics.median(baseline_values)
    after = statistics.median(candidate_values)
    return dict(
        pairs=len(trials),
        baseline_median=before,
        candidate_median=after,
        ratio=after / before,
        change_percent=100.0 * (after / before - 1.0),
        paired_ratios=paired_ratios,
        paired_percent=[100.0 * (ratio - 1.0) for ratio in paired_ratios],
        baseline_min=min(baseline_values),
        baseline_max=max(baseline_values),
        candidate_min=min(candidate_values),
        candidate_max=max(candidate_values),
    )
