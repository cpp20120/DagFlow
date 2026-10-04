#!/usr/bin/env python3
"""Compare frozen baseline/drain/wake/combined main harness snapshots."""
import argparse
import csv
import itertools
import json
from pathlib import Path
import random
import shutil
import statistics

from benchmark_main import Runner, checked_output, digest, physical_cpus, save, EVENT_GROUPS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='contains sources/<variant> snapshots')
    parser.add_argument('--name', default='comparison')
    parser.add_argument('--variants', default='baseline,drain,wake,combined')
    parser.add_argument('--profiles', default='o3')
    parser.add_argument('--rounds', type=int, default=5)
    parser.add_argument('--warmup-ms', type=int, default=50)
    parser.add_argument('--min-ms', type=int, default=50)
    parser.add_argument('--diagnostics', action='store_true')
    parser.add_argument('--counters', action='store_true')
    args = parser.parse_args()
    root = args.directory.resolve()
    out = root / args.name
    out.mkdir()
    variants, profiles = args.variants.split(','), args.profiles.split(',')
    if 'baseline' not in variants or args.rounds < 1:
        parser.error('comparison requires baseline and positive rounds')
    if not set(variants) <= {'baseline', 'drain', 'wake', 'combined', 'local_batch', 'local_batch_wake', 'drain_retry', 'drain_retry_wake', 'wake_once', 'relay'} or not set(profiles) <= {'o3', 'o3-lto'}:
        parser.error('unknown variant/profile')
    cores, topology = physical_cpus()
    if len(cores) < 6:
        parser.error('six allowed physical cores required for this experiment')
    cases = [dict(scenario=s, workers=w) for s, w in itertools.product(
        ('external-contention', 'external-batch', 'hot-shard-skew', 'local-overflow',
         'nested-helping', 'mixed-chaos', 'idle-burst'), (1, 4))]
    cases += [dict(scenario=s, workers=4, central_batch=32)
              for s in ('external-contention', 'external-batch')]
    cases += [dict(scenario='external-contention', workers=4, iterations=64),
              dict(scenario='external-batch', workers=4, iterations=64),
              dict(scenario='external-contention', workers=4, shards=1, producers=4)]
    for c in cases:
        c.setdefault('producers', 2)
        c['worker_cpus'] = ','.join(map(str, cores[:c['workers']]))
        # Four producers intentionally share these same two reserved cores.
        c['producer_cpus'] = ','.join(map(str, cores[4:6]))
    runner = Runner(out, 180)
    shutil.copy2(__file__, out / 'runner.py')
    manifest = dict(cases=cases, cpu_topology=topology, variants=variants,
                    profiles=profiles, rounds=args.rounds, seed=1729,
                    source_sha256={}, binaries={})
    binaries = {}
    main_hashes = set()
    for variant in variants:
        source = root / 'sources' / variant
        main_hashes.add(digest(source / 'src/main.cpp'))
        manifest['source_sha256'][variant] = {
            str(p.relative_to(source)): digest(p) for p in source.rglob('*') if p.is_file()}
    if len(main_hashes) != 1:
        raise RuntimeError('harness differs between variants')
    save(out / 'manifest.json', manifest)
    for profile, variant, diagnostic in itertools.product(profiles, variants,
            (False, True) if args.diagnostics else (False,)):
        label = f'{profile}-{variant}' + ('-diag' if diagnostic else '')
        build = root / 'build' / label
        runner.command(['cmake', '-S', root / 'sources' / variant, '-B', build, '-G', 'Ninja',
            '-DCMAKE_CXX_COMPILER=clang++', '-DCMAKE_BUILD_TYPE=Release',
            '-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g -DNDEBUG',
            '-DDAGFLOW_BUILD_STATIC=ON', '-DDAGFLOW_BUILD_SHARED=OFF', '-DDAGFLOW_INSTALL=OFF',
            '-DDAGFLOW_BUILD_TESTS=OFF', '-DDAGFLOW_BUILD_EXAMPLES=ON', '-DDAGFLOW_USE_LLD=ON',
            '-DDAGFLOW_ALLOCATOR=mimalloc',
            '-DDAGFLOW_LTO_MODE=' + ('full' if profile == 'o3-lto' else 'none'),
            '-DDAGFLOW_RUNTIME_DIAGNOSTICS=' + ('ON' if diagnostic else 'OFF')], 'configure-' + label)
        runner.command(['cmake', '--build', build, '--target', 'DagFlow_example', '-j', '4'], 'build-' + label)
        binary = build / 'dagflow-example'
        binaries[profile, variant, diagnostic] = binary
        manifest['binaries'][label] = dict(path=str(binary), sha256=digest(binary))
        save(out / 'manifest.json', manifest)
        print('Built ' + label, flush=True)

    def command(profile, variant, case, diagnostic=False, timing=False):
        argv = [binaries[profile, variant, diagnostic], '--json', '--verify', 'checksum',
                '--warmup', '1', '--warmup-ms', args.warmup_ms, '--repeats', '7',
                '--bursts', '64', '--min-ms', args.min_ms if timing and case['scenario'] != 'idle-burst' else 0]
        for key, value in case.items():
            argv.extend(['--' + key.replace('_', '-'), value])
        return argv

    runs = {}
    jobs = list(itertools.product(range(args.rounds), range(len(cases)), profiles, variants))
    random.Random(1729).shuffle(jobs)
    for repeat, index, profile, variant in jobs:
        label = f'timing-c{index}-{profile}-{variant}-r{repeat}'
        _, output = runner.command(command(profile, variant, cases[index], timing=True), label)
        result = checked_output(output)
        if result['diagnostics']:
            raise RuntimeError('instrumentation enabled in timing baseline')
        save(out / (label + '.json'), result)
        runs.setdefault((index, profile, variant), []).append(result)
    rows = []
    for index, case in enumerate(cases):
        for profile, variant in itertools.product(profiles, variants):
            results = runs[index, profile, variant]
            medians = [r['run_p50_us'] for r in results]
            baseline = statistics.median(r['run_p50_us'] for r in runs[index, profile, 'baseline'])
            median = statistics.median(medians)
            row = dict(case=index, profile=profile, variant=variant, scenario=case['scenario'],
                       workers=case['workers'], producers=case['producers'],
                       central_batch=case.get('central_batch', 1024), iterations=case.get('iterations'),
                       shards=case.get('shards', case['workers']), median_us=median,
                       min_us=min(medians), max_us=max(medians), change_pct=(median/baseline-1)*100)
            label = f'c{index}-{profile}-{variant}'
            if args.diagnostics:
                _, output = runner.command(command(profile, variant, case, diagnostic=True), 'diag-' + label)
                result = checked_output(output)
                save(out / ('diag-' + label + '.json'), result)
                row.update({'diag_' + k: v / (result['sample_count'] * result['logical_tasks']) for k, v in result['counts'].items()})
            if args.counters:
                for i, group in enumerate(EVENT_GROUPS):
                    path = out / f'stat-{label}-g{i}.csv'
                    _, output = runner.command(['perf', 'stat', '-x', ';', '-o', path,
                        '-e', ','.join(group), '--', *command(profile, variant, case)],
                        f'stat-{label}-g{i}', gated=True)
                    save(out / f'stat-{label}-g{i}.json', checked_output(output))
            rows.append(row)
        save(out / 'summary.json', rows)
        print('Collected case', index, case['scenario'], case['workers'], flush=True)
    with (out / 'summary.csv').open('w') as file:
        writer = csv.DictWriter(file, fieldnames=list(dict.fromkeys(k for r in rows for k in r)))
        writer.writeheader()
        writer.writerows(rows)
    report = ['# Drain / wake experiment', '',
              'Negative change = faster. Median of independent process medians; identical harness and CPU masks.', '',
              '| Case | Profile | Variant | Scenario | Workers | Median us | Change | Range of process medians, us |',
              '|---|---|---|---|---:|---:|---:|---:|']
    for r in rows:
        report.append(f'| {r["case"]} | {r["profile"]} | {r["variant"]} | {r["scenario"]} | {r["workers"]} | '
                      f'{r["median_us"]:.2f} | {r["change_pct"]:+.1f}% | {r["min_us"]:.2f}–{r["max_us"]:.2f} |')
    (out / 'report.md').write_text('\n'.join(report) + '\n')
    print(out / 'report.md', flush=True)


if __name__ == '__main__':
    main()
