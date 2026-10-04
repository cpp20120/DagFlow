#!/usr/bin/env python3
"""Recheck graph regressions using the exact binaries from a saved comparison."""
import argparse
import csv
import json
import os
from pathlib import Path
import random
import resource
import shutil
import statistics

from benchmark_main import Runner, digest, physical_cpus, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=15)
    parser.add_argument('--repeats', type=int, default=201)
    parser.add_argument('--warmup', type=int, default=30)
    args = parser.parse_args()
    if args.rounds < 2 or args.repeats < 1 or args.warmup < 0:
        parser.error('invalid repetition count')
    source = args.comparison.resolve()
    previous = json.loads((source / 'manifest.json').read_text())
    old_summary = json.loads((source / 'summary.json').read_text())
    # Include every positive change, however small, plus the empty four-worker
    # chain as an improvement control in both profiles. Selection is fixed
    # before measuring; no dropping inconvenient pairs or outliers afterward.
    points = [r for r in old_summary if r['change_percent'] > 0 or
              (r['scenario'] == 'deep_dag' and r['workers'] == 4 and r['iterations'] == 0)]
    binaries = {name: Path(item['path']) for name, item in previous['binaries'].items()}
    for name, binary in binaries.items():
        if digest(binary) != previous['binaries'][name]['sha256']:
            parser.error('binary differs from original measurement: ' + name)
    # Reuse the exact per-case CPU mask rather than selecting new cores.
    masks = {r['case']: r['cpus'] for r in
             map(json.loads, (source / 'runs.jsonl').read_text().splitlines())}
    allowed = os.sched_getaffinity(0)
    if any(not set(map(int, masks[r['case']].split(','))) <= allowed for r in points):
        parser.error('original CPU mask is no longer allowed')
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    runner = Runner(out, 180)
    shutil.copy2(__file__, out / 'runner.py')
    manifest = dict(original_comparison=str(source),
                    original_manifest_sha256=digest(source / 'manifest.json'),
                    binaries=previous['binaries'], points=points, masks=masks,
                    rounds=args.rounds, repeats=args.repeats, warmup=args.warmup,
                    seed=20261002, cpu_topology=physical_cpus()[1],
                    placement=previous['placement'], frequency_locked=False,
                    loadavg_start=os.getloadavg())
    save(out / 'manifest.json', manifest)
    jobs = [(trial, point) for trial in range(args.rounds) for point in range(len(points))]
    rng = random.Random(manifest['seed'])
    rng.shuffle(jobs)
    rows = []
    usage_fields = ('ru_utime', 'ru_stime', 'ru_nvcsw', 'ru_nivcsw', 'ru_minflt', 'ru_majflt')
    with (out / 'runs.jsonl').open('w') as raw:
        for number, (trial, point) in enumerate(jobs):
            case = points[point]
            order = ['baseline', 'candidate']
            rng.shuffle(order)
            for position, variant in enumerate(order):
                profile = case['profile']
                label = f'r{trial}-p{point}-{profile}-{variant}'
                argv = ['taskset', '-c', masks[case['case']], binaries[profile + '-' + variant],
                        '--repeats', args.repeats, '--warmup', args.warmup]
                for key in ('scenario', 'workers', 'tasks', 'iterations'):
                    argv.extend(['--' + key, case[key]])
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                _, stdout = runner.command(argv, label)
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                row = json.loads(stdout)
                if row['status'] != 'ok':
                    raise RuntimeError(label + ' failed')
                row.update(trial=trial, point=point, case=case['case'], profile=profile,
                           variant=variant, position_in_pair=position, cpus=masks[case['case']],
                           process_usage={k: getattr(after, k) - getattr(before, k) for k in usage_fields})
                rows.append(row)
                raw.write(json.dumps(row) + '\n')
                raw.flush()
            if (number + 1) % 15 == 0:
                print(f'Pairs {number + 1}/{len(jobs)}', flush=True)
    summary = []
    bootstrap_rng = random.Random(9187)
    for point, case in enumerate(points):
        group = [r for r in rows if r['point'] == point]
        if len({r['checksum'] for r in group}) != 1:
            raise RuntimeError('checksum mismatch')
        samples = {v: {r['trial']: r['run_p50_us'] for r in group if r['variant'] == v}
                   for v in ('baseline', 'candidate')}
        paired = [100 * (samples['candidate'][i] / samples['baseline'][i] - 1)
                  for i in range(args.rounds)]
        bootstrap = sorted(statistics.median(bootstrap_rng.choices(paired, k=len(paired)))
                           for _ in range(10000))
        before, after = (statistics.median(samples[v].values()) for v in ('baseline', 'candidate'))
        summary.append(dict(point=point, case=case['case'], profile=case['profile'],
                            **{k: case[k] for k in ('scenario', 'workers', 'tasks', 'iterations')},
                            previous_change_percent=case['change_percent'],
                            baseline_us=before, candidate_us=after, change_percent=100 * (after / before - 1),
                            paired_median_percent=statistics.median(paired),
                            paired_ci_low=bootstrap[249], paired_ci_high=bootstrap[9749],
                            candidate_slower_pairs=sum(p > 0 for p in paired),
                            baseline_min_us=min(samples['baseline'].values()), baseline_max_us=max(samples['baseline'].values()),
                            candidate_min_us=min(samples['candidate'].values()), candidate_max_us=max(samples['candidate'].values())))
    save(out / 'summary.json', summary)
    with (out / 'summary.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    report = ['# Graph regression recheck', '',
              f'Exact original binary hashes and CPU masks; {args.rounds} paired processes per variant, '
              f'{args.warmup} warmups and {args.repeats} measured repetitions per process.',
              'No outlier removal. Positive change means slower. CI: percentile bootstrap 95% interval '
              'for the median paired percentage (10,000 resamples); exploratory, no multiple-comparison correction.', '',
              '| Scenario | W | Tasks | Work iterations | Profile | Previous | Recheck | Paired median [95% CI] | Slower pairs | Baseline → candidate µs |',
              '|---|---:|---:|---:|---|---:|---:|---:|---:|---:|']
    for r in summary:
        report.append(f"| {r['scenario']} | {r['workers']} | {r['tasks']} | {r['iterations']} | {r['profile']} | "
                      f"{r['previous_change_percent']:+.1f}% | {r['change_percent']:+.1f}% | "
                      f"{r['paired_median_percent']:+.1f}% [{r['paired_ci_low']:+.1f}, {r['paired_ci_high']:+.1f}] | "
                      f"{r['candidate_slower_pairs']}/{args.rounds} | {r['baseline_us']:.2f} → {r['candidate_us']:.2f} |")
    (out / 'report.md').write_text('\n'.join(report) + '\n')
    manifest.update(loadavg_end=os.getloadavg(), cpu_topology_end=physical_cpus()[1])
    save(out / 'manifest.json', manifest)
    print(out / 'report.md', flush=True)


if __name__ == '__main__':
    main()
