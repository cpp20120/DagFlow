#!/usr/bin/env python3
"""Profile bench/stress_harness.cpp workloads, keeping uninstrumented timing separate from perf."""
import argparse
import csv
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path

import random
import re
import shutil
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = ('external-contention', 'hot-shard-skew', 'local-overflow',
             'nested-helping', 'mixed-chaos', 'idle-burst')
EVENTS = ('cycles:u', 'instructions:u', 'branches:u', 'branch-misses:u',
          'cache-references:u', 'cache-misses:u', 'page-faults')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_timing(stdout, scenario):
    line = next(line for line in stdout.splitlines() if line.startswith(scenario + ' '))
    numbers = {k: float(v) for k, v in re.findall(r'([a-z0-9]+)=\s*([0-9.]+)', line)}
    if scenario == 'idle-burst':
        return dict(time_us=numbers['p50'], p99_us=numbers['p99'], max_us=numbers['max'])
    return dict(time_us=1000 * numbers['median'], min_us=1000 * numbers['min'],
                max_us=1000 * numbers['max'], mtasks_s=numbers['throughput'])


def parse_stat(path):
    counts, running = {}, {}
    for row in csv.reader(path.read_text().splitlines(), delimiter=';'):
        if len(row) >= 3 and row[2] == 'page-faults:u':
            row[2] = 'page-faults'
        if len(row) < 3 or row[2] not in EVENTS:
            continue
        if row[0].startswith('<'):
            raise RuntimeError(f'Counter unavailable: {row}')
        counts[row[2]] = float(row[0])
        if len(row) > 4 and row[4]:
            running[row[2]] = float(row[4])
    if set(counts) != set(EVENTS):
        raise RuntimeError(f'Incomplete counters: {path}: {counts}')
    return counts, running


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--trials', type=int, default=5)
    parser.add_argument('--stat-trials', type=int, default=3)
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--resume', action='store_true',
                        help='reuse successful commands and verified source/binaries in output')
    args = parser.parse_args()
    if min(args.trials, args.stat_trials, args.jobs) < 1:
        parser.error('counts must be positive')
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if not args.resume and ((out / 'manifest.json').exists() or (out / 'source').exists()):
        parser.error('output already contains a measurement')
    logs = out / 'logs'
    logs.mkdir(exist_ok=True)
    commands = json.loads((out / 'commands.json').read_text()) if args.resume else []
    environment = dict(os.environ, LC_ALL='C', DEBUGINFOD_URLS='')

    def command(argv, label, timeout=300):
        argv = list(map(str, argv))
        if args.resume:
            previous = next((r for r in reversed(commands) if r['label'] == label), None)
            if previous and previous.get('returncode') == 0 and previous['argv'] == argv:
                return (logs / (label + '.stdout')).read_text()
        record = dict(argv=argv, label=label, started_unix=time.time())
        commands.append(record)
        try:
            result = subprocess.run(argv, capture_output=True, text=True,
                                    env=environment, timeout=timeout)
            (logs / (label + '.stdout')).write_text(result.stdout)
            (logs / (label + '.stderr')).write_text(result.stderr)
            record['returncode'] = result.returncode
            if result.returncode:
                raise RuntimeError(f'{label} failed: {result.stderr[-3000:]} {result.stdout[-1000:]}')
            return result.stdout
        finally:
            record['elapsed_seconds'] = time.time() - record['started_unix']
            (out / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')

    source = out / 'source'
    if not args.resume:
        source.mkdir()
        for directory in ('include', 'src', 'cmake', 'bench'):
            shutil.copytree(ROOT / directory, source / directory)
        shutil.copy2(ROOT / 'CMakeLists.txt', source)
    cpus = ','.join(map(str, sorted(os.sched_getaffinity(0))))
    manifest = dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    source='bench/stress_harness.cpp only; scenario selection; original workload counts',
                    cpus=cpus, pin_threads=False, allocator='mimalloc',
                    timing_trials=args.trials, stat_trials=args.stat_trials,
                    events=EVENTS, record_event='cycles:u',
                    call_graph='dwarf,16384', frequency_locked=False,
                    build='static DagFlow, dynamic mimalloc; O3 -g -DNDEBUG; lld for both',
                    cpu=command(['lscpu'], 'cpu'), compiler=command(['clang++', '--version'], 'compiler'),
                    perf=command(['perf', 'version', '--build-options'], 'perf-version'),
                    source_sha256={str(p.relative_to(source)): digest(p)
                                   for p in sorted(source.rglob('*')) if p.is_file()})
    if args.resume:
        previous = json.loads((out / 'manifest.json').read_text())
        for field in ('source_sha256', 'cpus', 'timing_trials', 'stat_trials'):
            if previous[field] != manifest[field]:
                raise RuntimeError('Cannot resume changed ' + field)
        for binary in previous.get('binaries', {}).values():
            if digest(Path(binary['path'])) != binary['sha256']:
                raise RuntimeError('Cannot resume modified binary')
        manifest = previous
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    binaries = {}
    for profile, lto in (('o3', 'none'), ('o3-lto', 'full')):
        build = out / 'build' / profile
        command(['cmake', '-S', source, '-B', build, '-G', 'Ninja',
                 '-DCMAKE_CXX_COMPILER=clang++', '-DCMAKE_BUILD_TYPE=Release',
                 '-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g -DNDEBUG',
                 '-DDAGFLOW_BUILD_SHARED=OFF', '-DDAGFLOW_BUILD_STATIC=ON', '-DDAGFLOW_INSTALL=OFF',
                 '-DDAGFLOW_BUILD_EXAMPLES=OFF', '-DDAGFLOW_BUILD_STRESS_BENCH=ON', '-DDAGFLOW_BUILD_TESTS=OFF',
                 '-DDAGFLOW_USE_LLD=ON', '-DDAGFLOW_ENABLE_NATIVE=OFF',
                 '-DDAGFLOW_PGO_MODE=none', '-DDAGFLOW_ALLOCATOR=mimalloc',
                 '-DDAGFLOW_LTO_MODE=' + lto], profile + '-configure')
        command(['cmake', '--build', build, '--target', 'dagflow_stress_bench', '-j', args.jobs],
                profile + '-build')
        binaries[profile] = build / 'dagflow-stress-bench'
        command(['ldd', binaries[profile]], profile + '-ldd')
        print('Built ' + profile, flush=True)
    manifest['binaries'] = {k: dict(path=str(v), sha256=digest(v)) for k, v in binaries.items()}
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

    def invocation(profile, workers, scenario):
        return ['taskset', '-c', cpus, binaries[profile], '--workers', workers, '--scenario', scenario]

    points = list(itertools.product((1, 4), SCENARIOS))
    rng = random.Random(20260930)
    timings, counters = [], []
    with (out / 'timings.jsonl').open('w') as raw:
        for trial in range(args.trials):
            rng.shuffle(points)
            for workers, scenario in points:
                order = ['o3', 'o3-lto']
                rng.shuffle(order)
                for profile in order:
                    label = f'time-{trial}-{profile}-{workers}-{scenario}'
                    stdout = command(invocation(profile, workers, scenario), label)
                    row = dict(profile=profile, workers=workers, scenario=scenario, trial=trial,
                               **parse_timing(stdout, scenario))
                    timings.append(row)
                    raw.write(json.dumps(row) + '\n')
                    raw.flush()
            print(f'Timing round {trial + 1}/{args.trials}', flush=True)
    with (out / 'counters.jsonl').open('w') as raw:
        for trial in range(args.stat_trials):
            rng.shuffle(points)
            for workers, scenario in points:
                order = ['o3', 'o3-lto']
                rng.shuffle(order)
                for profile in order:
                    label = f'stat-{trial}-{profile}-{workers}-{scenario}'
                    path = out / (label + '.csv')
                    command(['perf', 'stat', '-x', ';', '-o', path, '-e', ','.join(EVENTS), '--',
                             *invocation(profile, workers, scenario)], label)
                    counts, running = parse_stat(path)
                    row = dict(profile=profile, workers=workers, scenario=scenario, trial=trial,
                               counts=counts, running_pct=running)
                    counters.append(row)
                    raw.write(json.dumps(row) + '\n')
                    raw.flush()
            print(f'Counter round {trial + 1}/{args.stat_trials}', flush=True)

    manifest['sampling'] = 'fixed period max(10000, median stat cycles / 1500); weighted by period'
    manifest.pop('record_frequency_hz', None)
    manifest['profile_periods'] = {}
    handler = out / 'perf_flamegraph.py'
    shutil.copy2(ROOT / 'tools/profiling/perf_flamegraph.py', handler)
    for profile, workers, scenario in itertools.product(('o3', 'o3-lto'), (1, 4), SCENARIOS):
        label = f'{profile}-{workers}-{scenario}'
        directory = out / 'profiles' / label
        directory.mkdir(parents=True, exist_ok=True)
        data = directory / 'perf.data'
        matching = [r['counts']['cycles:u'] for r in counters if
                    (r['profile'], r['workers'], r['scenario']) == (profile, workers, scenario)]
        period = max(10000, int(statistics.median(matching) / 1500))
        manifest['profile_periods'][label] = period
        command(['perf', 'record', '-o', data, '-e', 'cycles:u', '-c', period,
                 '--call-graph', 'dwarf,16384', '--',
                 *invocation(profile, workers, scenario)], 'periodic-record-' + label)
        flat = command(['perf', 'report', '-i', data, '--stdio', '--no-children',
                        '--sort', 'symbol,dso', '--percent-limit', '0.5'], 'periodic-flat-' + label)
        (directory / 'hotspots.txt').write_text(flat)
        graph = command(['perf', 'report', '-i', data, '--stdio', '--children',
                         '--call-graph', 'graph,0.5,caller', '--percent-limit', '0.5'], 'periodic-callgraph-' + label)
        (directory / 'callgraph.txt').write_text(graph)
        command(['perf', 'script', '-i', data, '-s', handler, '--',
                 directory / 'flamegraph.json'], 'periodic-flame-' + label)
        print('Profiled ' + label, flush=True)

    manifest['flamegraph_handler_sha256'] = digest(handler)
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    summary = []
    for profile, workers, scenario in itertools.product(('o3', 'o3-lto'), (1, 4), SCENARIOS):
        selected = [r for r in timings if (r['profile'], r['workers'], r['scenario']) ==
                    (profile, workers, scenario)]
        counts = [r for r in counters if (r['profile'], r['workers'], r['scenario']) ==
                  (profile, workers, scenario)]
        row = dict(profile=profile, workers=workers, scenario=scenario,
                   time_us=statistics.median(r['time_us'] for r in selected),
                   min_trial_us=min(r['time_us'] for r in selected),
                   max_trial_us=max(r['time_us'] for r in selected))
        if scenario == 'idle-burst':
            row['p99_us'] = statistics.median(r['p99_us'] for r in selected)
        row.update({event: statistics.median(r['counts'][event] for r in counts) for event in EVENTS})
        row['ipc'] = statistics.median(r['counts']['instructions:u'] / r['counts']['cycles:u'] for r in counts)
        row['branch_miss_pct'] = statistics.median(100 * r['counts']['branch-misses:u'] / r['counts']['branches:u'] for r in counts)
        row['cache_miss_pct'] = statistics.median(100 * r['counts']['cache-misses:u'] / r['counts']['cache-references:u'] for r in counts)
        row['min_counter_running_pct'] = min(v for r in counts for v in r['running_pct'].values())
        if profile == 'o3-lto':
            before = {r['trial']: r['time_us'] for r in timings if
                      (r['profile'], r['workers'], r['scenario']) == ('o3', workers, scenario)}
            changes = [100 * (r['time_us'] / before[r['trial']] - 1) for r in selected]
            row.update(paired_time_change_pct=statistics.median(changes),
                       min_paired_change_pct=min(changes), max_paired_change_pct=max(changes))
        summary.append(row)
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    with (out / 'summary.csv').open('w') as f:
        keys = list(dict.fromkeys(k for row in summary for k in row))
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(summary)
    print('Complete: ' + str(out), flush=True)


if __name__ == '__main__':
    main()
