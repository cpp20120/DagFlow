#!/usr/bin/env python3
"""Linux paired benchmark of runtime STL allocator routing, with source snapshots."""
import argparse
import csv
import datetime
import difflib
import hashlib
import itertools
import json
import os
from pathlib import Path

from benchmark_build import source_options
import random
import shutil
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
# Reverse only the allocator routing; runtime code, zero-size handling, and
# the benchmark are identical in both snapshots. No git checkout is involved.
REPLACEMENTS = {
    'include/dagflow/thread_pool.hpp': ['uint32_t', 'std::thread'],
    'include/dagflow/task_graph.hpp': ['BuilderNode', 'Work'],
    'include/dagflow/graph_scope.hpp': ['Block'],
    'include/dagflow/handle.hpp': ['CompletionCredit'],
    'src/thread_pool.cpp': ['CompletionCredit'],
    'src/task_graph.cpp': ['index_type', 'Work'],
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=5)
    parser.add_argument('--repeats', type=int, default=31)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--jobs', type=int, default=4)
    args = parser.parse_args()
    if min(args.trials, args.repeats, args.jobs) < 1 or args.warmup < 0:
        parser.error('invalid repetition/build count')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    logs = output / 'logs'
    logs.mkdir()
    commands = []

    def command(argv, label, timeout=180):
        argv = source_options(argv)
        started = time.time()
        record = dict(argv=argv, label=label, started_unix=started)
        commands.append(record)
        try:
            result = subprocess.run(argv, text=True, capture_output=True, timeout=timeout)
            record['returncode'] = result.returncode
            (logs / (label + '.stdout')).write_text(result.stdout)
            (logs / (label + '.stderr')).write_text(result.stderr)
            if result.returncode:
                raise RuntimeError(f'{label} failed: {result.stderr[-2000:]}')
            return result.stdout
        finally:
            record['elapsed_seconds'] = time.time() - started
            (output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')

    snapshots = {}
    for version in ('before', 'after'):
        source = output / 'source' / version
        source.mkdir(parents=True)
        for directory in ('include', 'src', 'bench', 'cmake'):
            shutil.copytree(ROOT / directory, source / directory)
        shutil.copy2(ROOT / 'CMakeLists.txt', source)
        snapshots[version] = source
    patch = []
    replaced = 0
    for name, types in REPLACEMENTS.items():
        path = snapshots['before'] / name
        after = path.read_text()
        before = after
        qualifier = '' if name in ('include/dagflow/handle.hpp', 'src/thread_pool.cpp') else 'detail::'
        for element in types:
            old = f'std::vector<{element}, {qualifier}RuntimeAllocator<{element}>>'
            if old not in before:
                raise RuntimeError(f'baseline recipe no longer matches {name}: {old}')
            replaced += before.count(old)
            before = before.replace(old, f'std::vector<{element}>')
        path.write_text(before)
        patch.extend(difflib.unified_diff(before.splitlines(True), after.splitlines(True),
                                         fromfile='before/' + name, tofile='after/' + name))
    if replaced != 10:
        raise RuntimeError(f'expected 10 allocator uses, found {replaced}')
    (output / 'allocator-routing.patch').write_text(''.join(patch))

    cores = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        key = tuple((topology / field).read_text().strip()
                    for field in ('physical_package_id', 'core_id'))
        cores.setdefault(key, cpu)
    cpus = list(cores.values())
    if len(cpus) < 5:
        raise RuntimeError('requires at least five available physical cores')
    manifest = dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    trials=args.trials, repeats=args.repeats, warmup=args.warmup,
                    physical_core_representatives=cpus, frequency_locked=False,
                    placement='workers+1 distinct physical cores, no per-thread pinning',
                    baseline='current snapshot with only 10 STL allocator uses reverted',
                    source_sha256={version: {str(p.relative_to(source)): sha(p)
                        for p in sorted(source.rglob('*')) if p.is_file()}
                        for version, source in snapshots.items()})
    manifest['compiler'] = command(['clang++', '--version'], 'compiler')
    manifest['cpu'] = command(['lscpu'], 'cpu')
    manifest['git_head'] = command(['git', '-C', ROOT, 'rev-parse', 'HEAD'], 'git-head').strip()
    manifest['git_status'] = command(['git', '-C', ROOT, 'status', '--short'], 'git-status')
    binaries = {}
    for backend, version in itertools.product(('system', 'mimalloc', 'tbbmalloc'), ('before', 'after')):
        label = backend + '-' + version
        build = output / 'build' / label
        command(['cmake', '-S', snapshots[version], '-B', build, '-G', 'Ninja',
                 '-DCMAKE_CXX_COMPILER=clang++', '-DCMAKE_BUILD_TYPE=Release',
                 '-DDAGFLOW_BUILD_SHARED=OFF', '-DDAGFLOW_BUILD_STATIC=ON', '-DDAGFLOW_INSTALL=OFF',
                 '-DDAGFLOW_BUILD_EXAMPLES=OFF', '-DDAGFLOW_BUILD_TESTS=OFF',
                 '-DDAGFLOW_BUILD_RUNTIME_BENCH=ON', '-DDAGFLOW_LTO_MODE=none',
                 '-DDAGFLOW_PGO_MODE=none', '-DDAGFLOW_ALLOCATOR=' + backend], label + '-configure')
        command(['cmake', '--build', build, '--target', 'dagflow_runtime_suite', '-j', args.jobs],
                label + '-build', timeout=300)
        binary = build / 'dagflow-runtime-suite'
        binaries[backend, version] = binary
        print('Built ' + label, flush=True)
    manifest['binaries'] = {b + '-' + v: {'path': str(p), 'sha256': sha(p)}
                           for (b, v), p in binaries.items()}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    points = list(itertools.product(('system', 'mimalloc', 'tbbmalloc'), (1, 4),
                  ('dag_build_run', 'graph_reuse', 'external_handles', 'external_detached'), (0, 400)))
    rng = random.Random(1947)
    rows = []
    with (output / 'paired.jsonl').open('w') as raw:
        for trial in range(args.trials):
            rng.shuffle(points)
            for backend, workers, scenario, iterations in points:
                order = ['before', 'after']
                rng.shuffle(order)
                mask = ','.join(map(str, cpus[:workers + 1]))
                pair = []
                for version in order:
                    label = f'{trial}-{backend}-{workers}-{scenario}-{iterations}-{version}'
                    row = json.loads(command(['taskset', '-c', mask, binaries[backend, version],
                        '--scenario', scenario, '--workers', workers, '--tasks', 4096,
                        '--iterations', iterations, '--repeats', args.repeats, '--warmup', args.warmup], label))
                    if row['status'] != 'ok' or row['allocator'] != backend:
                        raise RuntimeError(f'invalid result: {row}')
                    row.update(version=version, trial=trial, cpus=mask)
                    pair.append(row)
                    rows.append(row)
                    raw.write(json.dumps(row) + '\n')
                    raw.flush()
                if pair[0]['checksum'] != pair[1]['checksum']:
                    raise RuntimeError('before/after payload checksum mismatch')
            print(f'Trial {trial + 1}/{args.trials} complete', flush=True)
    summary = []
    for backend, workers, scenario, iterations in sorted(points):
        selected = [r for r in rows if (r['allocator'], r['workers'], r['scenario'], r['iterations']) ==
                    (backend, workers, scenario, iterations)]
        if len({r['checksum'] for r in selected}) != 1:
            raise RuntimeError('cross-trial payload checksum mismatch')
        before = {r['trial']: r['run_p50_us'] for r in selected if r['version'] == 'before'}
        after = {r['trial']: r['run_p50_us'] for r in selected if r['version'] == 'after'}
        changes = [100 * (after[t] / before[t] - 1) for t in range(args.trials)]
        summary.append(dict(backend=backend, workers=workers, scenario=scenario, iterations=iterations,
            before_us=statistics.median(before.values()), after_us=statistics.median(after.values()),
            paired_change_pct=statistics.median(changes), min_change_pct=min(changes),
            max_change_pct=max(changes), paired_changes=changes))
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    with (output / 'summary.csv').open('w') as out:
        writer = csv.DictWriter(out, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    print('Results: ' + str(output), flush=True)


if __name__ == '__main__':
    main()
