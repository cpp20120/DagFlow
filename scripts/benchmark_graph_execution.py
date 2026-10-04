#!/usr/bin/env python3
"""Compare frozen graph execution sources with an identical runtime suite."""
import argparse
import csv
import itertools
import json
from pathlib import Path
import random
import shutil
import statistics

from benchmark_main import Runner, digest, physical_cpus, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='contains sources/baseline and sources/candidate')
    parser.add_argument('--name', default='comparison')
    parser.add_argument('--rounds', type=int, default=5)
    parser.add_argument('--repeats', type=int, default=51)
    parser.add_argument('--warmup', type=int, default=10)
    args = parser.parse_args()
    if args.rounds < 1 or args.repeats < 1 or args.warmup < 0:
        parser.error('invalid repetition count')
    root = args.directory.resolve()
    variants = ('baseline', 'candidate')
    profiles = ('o3', 'o3-lto')
    if len({digest(root / 'sources' / v / 'bench/runtime_suite.cpp') for v in variants}) != 1:
        parser.error('benchmark source must be identical')
    cores, topology = physical_cpus()
    if len(cores) < 5:
        parser.error('five allowed physical cores required')
    out = root / args.name
    out.mkdir()
    runner = Runner(out, 180)
    shutil.copy2(__file__, out / 'runner.py')
    scenarios = ('deep_dag', 'fanout_fanin', 'graph_reuse', 'dag_build_run',
                 'graph_tokens_serial', 'graph_tokens_parallel')
    cases = [dict(scenario=s, workers=w, tasks=4096, iterations=i)
             for s, w, i in itertools.product(scenarios, (1, 4), (0, 128))]
    cases += [dict(scenario=s, workers=1, tasks=n, iterations=0)
              for s, n in itertools.product(('deep_dag', 'graph_tokens_serial'), (64, 65))]
    manifest = dict(cases=cases, variants=variants, profiles=profiles,
                    rounds=args.rounds, repeats=args.repeats, warmup=args.warmup,
                    seed=1729, cpu_topology=topology, source_sha256={}, binaries={},
                    placement='process restricted to workers+1 physical cores; threads not individually pinned',
                    frequency_locked=False)
    binaries = {}
    for variant in variants:
        source = root / 'sources' / variant
        manifest['source_sha256'][variant] = {
            str(p.relative_to(source)): digest(p) for p in source.rglob('*') if p.is_file()}
    save(out / 'manifest.json', manifest)
    for profile, variant in itertools.product(profiles, variants):
        label = f'{profile}-{variant}'
        build = root / 'build' / label
        runner.command(['cmake', '-S', root / 'sources' / variant, '-B', build, '-G', 'Ninja',
            '-DCMAKE_CXX_COMPILER=clang++', '-DCMAKE_BUILD_TYPE=Release',
            '-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g -DNDEBUG', '-DDAGFLOW_ALLOCATOR=mimalloc',
            '-DDAGFLOW_BUILD_STATIC=ON', '-DDAGFLOW_BUILD_SHARED=OFF', '-DDAGFLOW_INSTALL=OFF',
            '-DDAGFLOW_BUILD_TESTS=OFF', '-DDAGFLOW_BUILD_EXAMPLES=OFF', '-DDAGFLOW_BUILD_RUNTIME_BENCH=ON',
            '-DDAGFLOW_USE_LLD=ON',
            '-DDAGFLOW_LTO_MODE=' + ('full' if profile == 'o3-lto' else 'none')], 'configure-' + label)
        runner.command(['cmake', '--build', build, '--target', 'dagflow_runtime_suite', '-j', '4'], 'build-' + label)
        binary = build / 'dagflow-runtime-suite'
        binaries[profile, variant] = binary
        manifest['binaries'][label] = dict(path=str(binary), sha256=digest(binary))
        save(out / 'manifest.json', manifest)
        print('Built ' + label, flush=True)
    jobs = list(itertools.product(range(args.rounds), range(len(cases)), profiles))
    rng = random.Random(1729)
    rng.shuffle(jobs)
    rows = []
    with (out / 'runs.jsonl').open('w') as raw:
        for number, (trial, case, profile) in enumerate(jobs):
            order = list(variants)
            rng.shuffle(order)
            c = cases[case]
            mask = ','.join(map(str, cores[:c['workers'] + 1]))
            for variant in order:
                label = f'r{trial}-c{case}-{profile}-{variant}'
                argv = ['taskset', '-c', mask, binaries[profile, variant],
                        '--repeats', args.repeats, '--warmup', args.warmup]
                for key, value in c.items():
                    argv.extend(['--' + key, value])
                _, output = runner.command(argv, label)
                row = json.loads(output)
                if row['status'] != 'ok':
                    raise RuntimeError(label + ' did not succeed')
                row.update(trial=trial, case=case, profile=profile, variant=variant, cpus=mask)
                rows.append(row)
                raw.write(json.dumps(row) + '\n')
                raw.flush()
            if (number + 1) % 20 == 0:
                print(f'Timing pairs {number + 1}/{len(jobs)}', flush=True)
    summary = []
    for case, profile in itertools.product(range(len(cases)), profiles):
        group = [r for r in rows if r['case'] == case and r['profile'] == profile]
        if len({r['checksum'] for r in group}) != 1:
            raise RuntimeError('checksum mismatch')
        medians = {v: [r['run_p50_us'] for r in group if r['variant'] == v] for v in variants}
        before, after = (statistics.median(medians[v]) for v in variants)
        summary.append(dict(case=case, profile=profile, **cases[case], baseline_us=before,
                            candidate_us=after, change_percent=(after / before - 1) * 100,
                            baseline_min_us=min(medians['baseline']), baseline_max_us=max(medians['baseline']),
                            candidate_min_us=min(medians['candidate']), candidate_max_us=max(medians['candidate'])))
    save(out / 'summary.json', summary)
    with (out / 'summary.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    report = ['# Graph execution comparison', '',
              'Median of five process medians (or --rounds); negative change means faster.',
              'Ranges are minima/maxima of process medians, not confidence intervals.', '',
              '| Scenario | W | Tasks | Iterations | Profile | Baseline µs | Candidate µs | Change | Baseline range µs | Candidate range µs |',
              '|---|---:|---:|---:|---|---:|---:|---:|---:|---:|']
    for r in summary:
        report.append(f"| {r['scenario']} | {r['workers']} | {r['tasks']} | {r['iterations']} | {r['profile']} | "
                      f"{r['baseline_us']:.2f} | {r['candidate_us']:.2f} | {r['change_percent']:+.1f}% | "
                      f"{r['baseline_min_us']:.2f}–{r['baseline_max_us']:.2f} | "
                      f"{r['candidate_min_us']:.2f}–{r['candidate_max_us']:.2f} |")
    (out / 'report.md').write_text('\n'.join(report) + '\n')
    print(out / 'report.md', flush=True)


if __name__ == '__main__':
    main()
