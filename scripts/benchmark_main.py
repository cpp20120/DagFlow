#!/usr/bin/env python3
"""Reproducible, phase-gated profiles of src/main.cpp (Linux)."""
import argparse
import csv
import itertools
import json
import os
from pathlib import Path
import sys

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from benchmark_build import source_options
from dagflow_harness_bridge import CommandRunner, digest, physical_cpus
import random
import shutil
import signal
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = ('external-contention', 'external-batch', 'hot-shard-skew',
             'local-overflow', 'nested-helping', 'mixed-chaos', 'idle-burst')
EVENT_GROUPS = (
    ('cycles:u', 'instructions:u', 'branches:u', 'branch-misses:u'),
    ('cache-references:u', 'cache-misses:u'),
    ('task-clock', 'context-switches', 'cpu-migrations', 'page-faults',
     'minor-faults', 'major-faults'),
)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def parse_stat(path):
    """Keep unsupported/not-counted events distinct from actual zero counts."""
    result = {}
    for line in path.read_text().splitlines():
        cells = line.split(';')
        if len(cells) < 3 or line.startswith('#'):
            continue
        value, unit, event = (cell.strip() for cell in cells[:3])
        if not event:
            continue
        try:
            count = float(value)
        except ValueError:
            count = None
        try:
            running = float(cells[4].strip().rstrip('%'))
        except (ValueError, IndexError):
            running = None
        result[event] = dict(value=count, unit=unit, running_percent=running,
                             status='ok' if count is not None else value)
    return result


class Runner:
    def __init__(self, out, timeout):
        self.out, self.timeout, self.commands = out, timeout, []
        self.env = dict(os.environ, LC_ALL='C', DEBUGINFOD_URLS='')
        self.processes = CommandRunner(out, timeout, self.env)
        self.commands = self.processes.commands

    def command(self, argv, label, gated=False, required=True):
        argv = source_options(argv)
        if not gated:
            return self.processes.command(argv, label, required=required)
        descriptors = []
        if gated:
            cr, cw = os.pipe()
            ar, aw = os.pipe()
            descriptors = [cr, cw, ar, aw]
            separator = argv.index('--')
            argv[separator:separator] = ['-D', '-1', f'--control=fd:{cr},{aw}']
            argv += ['--perf-control-fd', str(cw), '--perf-ack-fd', str(ar)]
        entry = dict(label=label, argv=argv, started_unix=time.time())
        self.commands.append(entry)
        try:
            with (self.out / 'logs' / (label + '.stdout')).open('w') as stdout, \
                 (self.out / 'logs' / (label + '.stderr')).open('w') as stderr:
                child = subprocess.Popen(argv, stdout=stdout, stderr=stderr,
                                         env=self.env, pass_fds=descriptors,
                                         start_new_session=True)
                for fd in descriptors:
                    os.close(fd)
                descriptors.clear()
                try:
                    entry['returncode'] = child.wait(timeout=self.timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                    entry['returncode'] = -signal.SIGKILL
                    raise
            if required and entry['returncode']:
                raise RuntimeError(f'{label} failed; see {self.out / "logs" / (label + ".stderr")}')
            return entry['returncode'], (self.out / 'logs' / (label + '.stdout')).read_text()
        finally:
            for fd in descriptors:
                os.close(fd)
            entry['elapsed_seconds'] = time.time() - entry['started_unix']
            save(self.out / 'commands.json', self.commands)


def cases_for(args):
    if args.cases:
        cases = json.loads(args.cases.read_text())
        if not isinstance(cases, list) or not cases:
            raise ValueError('--cases must contain a nonempty JSON array')
    else:
        names = SCENARIOS if args.scenarios == 'all' else args.scenarios.split(',')
        cases = [dict(scenario=s, workers=w, producers=p, shards=h,
                      submit_batch=b, iterations=i)
                 for s, w, p, h, b, i in itertools.product(
                     names, map(int, args.workers.split(',')),
                     map(int, args.producers.split(',')), map(int, args.shards.split(',')),
                     map(int, args.batches.split(',')),
                     [None if x == 'native' else int(x) for x in args.iterations.split(',')])]
    permitted = {'scenario', 'workers', 'producers', 'shards', 'submit_batch', 'iterations',
                 'tasks', 'central_batch', 'fanout', 'high_every', 'placement',
                 'producer_mode', 'idle_us', 'worker_cpus', 'producer_cpus'}
    unique = []
    for case in cases:
        if not isinstance(case, dict) or set(case) - permitted:
            raise ValueError('invalid case keys')
        case = dict(scenario='external-contention', workers=4, producers=2,
                    shards=0, submit_batch=0) | case
        if case['scenario'] not in SCENARIOS or not 1 <= case['workers'] <= 256 or not 1 <= case['producers'] <= 256:
            raise ValueError('invalid scenario/worker/producer count')
        if args.tasks:
            case.setdefault('tasks', args.tasks)
        if case not in unique:
            unique.append(case)
    return unique


def checked_output(output):
    lines = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
    if len(lines) != 1 or lines[0]['status'] != 'ok' or not lines[0]['duration_satisfied']:
        raise RuntimeError('missing/failed harness result or minimum duration not reached')
    return lines[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True, help='new output directory')
    parser.add_argument('--cases', type=Path, help='JSON list, overrides matrix options')
    parser.add_argument('--scenarios', default='all')
    parser.add_argument('--workers', default='1,4')
    parser.add_argument('--producers', default='2')
    parser.add_argument('--shards', default='0')
    parser.add_argument('--batches', default='0')
    parser.add_argument('--iterations', default='native')
    parser.add_argument('--tasks', type=int, default=0, help='0 uses scenario defaults')
    parser.add_argument('--profiles', default='o3,o3-lto')
    parser.add_argument('--allocator', choices=('system', 'mimalloc', 'tbbmalloc'), default='mimalloc')
    parser.add_argument('--compiler', default='clang++')
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--rounds', type=int, default=3, help='independent processes per timing case')
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--bursts', type=int, default=64, help='fixed sample count for idle-burst (no active-time extension)')
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument('--warmup-ms', type=int, default=200)
    parser.add_argument('--min-ms', type=int, default=200, help='accumulated measured time (timing only)')
    parser.add_argument('--profile-ms', type=int, default=500)
    parser.add_argument('--verify', choices=('off', 'checksum', 'exact'), default='checksum')
    parser.add_argument('--latency', action='store_true')
    parser.add_argument('--sample-stride', type=int, default=64)
    parser.add_argument('--perf', choices=('off', 'stat', 'all'), default='all')
    parser.add_argument('--extra-events', default='', help='comma-separated perf events, each measured in a separate pass')
    parser.add_argument('--frequency', type=int, default=199, help='perf record sample frequency')
    parser.add_argument('--idle-sample-period', type=int, default=10000, help='cycles per stack sample for short idle bursts')
    parser.add_argument('--no-diagnostics', action='store_true')
    parser.add_argument('--affinity', choices=('physical', 'inherit'), default='physical')
    parser.add_argument('--seed', type=int, default=1729)
    parser.add_argument('--timeout', type=int, default=300, help='timeout per command; kills child process group')
    args = parser.parse_args()
    profiles = list(dict.fromkeys(args.profiles.split(',')))
    if not set(profiles) <= {'o3', 'o3-lto'} or min(args.rounds, args.repeats, args.bursts, args.jobs, args.timeout, args.frequency, args.idle_sample_period) < 1:
        parser.error('invalid profile or nonpositive count')
    cases = cases_for(args)
    event_groups = [*EVENT_GROUPS, *((event,) for event in args.extra_events.split(',') if event)]
    cores, topology = physical_cpus()
    warnings = []
    if args.affinity == 'physical':
        max_workers = max(c['workers'] for c in cases)
        max_producers = max(c['producers'] for c in cases)
        if len(cores) < max_workers + max_producers:
            parser.error('insufficient physical cores for disjoint groups; use --affinity inherit or smaller counts')
        producer_cpus = ','.join(map(str, cores[max_workers:max_workers + max_producers]))
        for case in cases:
            case.setdefault('worker_cpus', ','.join(map(str, cores[:case['workers']])))
            case.setdefault('producer_cpus', producer_cpus)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = out / 'source'
    source.mkdir()
    # Build the frozen snapshot: edits to the working tree during a long sweep
    # cannot silently change the second profile or its diagnostics variant.
    for name in ('include', 'src', 'cmake', 'examples', 'scripts'):
        shutil.copytree(ROOT / name, source / name, ignore=shutil.ignore_patterns('__pycache__'))
    shutil.copy2(ROOT / 'CMakeLists.txt', source / 'CMakeLists.txt')
    runner = Runner(out, args.timeout)
    manifest = dict(schema=1, options={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                    cases=cases, cpu_topology=topology, uname=list(os.uname()), warnings=warnings,
                    source_sha256={str(p.relative_to(source)): digest(p) for p in source.rglob('*') if p.is_file()},
                    binaries={}, started_unix=time.time())
    save(out / 'manifest.json', manifest)
    for name, command in (('compiler', [args.compiler, '--version']), ('cpu', ['lscpu']),
                          ('perf', ['perf', '--version'])):
        if shutil.which(command[0]):
            runner.command(command, 'host-' + name, required=False)
    binaries = {}
    for profile in profiles:
        variants = ['baseline'] + ([] if args.no_diagnostics else ['diagnostics'])
        for variant in variants:
            label = profile + '-' + variant
            build = out / 'build' / label
            flags = 'CMake profile: ' + profile
            runner.command(['cmake', '-S', source, '-B', build, '-G', 'Ninja',
                            '-DCMAKE_CXX_COMPILER=' + args.compiler, '-DDAGFLOW_PROFILE=' + profile,
                            '-DDAGFLOW_BUILD_STATIC=ON', '-DDAGFLOW_BUILD_SHARED=OFF', '-DDAGFLOW_INSTALL=OFF',
                            '-DDAGFLOW_BUILD_TESTS=OFF', '-DDAGFLOW_BUILD_EXAMPLES=OFF', '-DDAGFLOW_BUILD_STRESS_BENCH=ON',
                            '-DDAGFLOW_USE_LLD=' + ('ON' if 'clang' in args.compiler else 'OFF'),
                            '-DDAGFLOW_ALLOCATOR=' + args.allocator,
                            '-DDAGFLOW_RUNTIME_DIAGNOSTICS=' + ('ON' if variant == 'diagnostics' else 'OFF')],
                           'configure-' + label)
            runner.command(['cmake', '--build', build, '--target', 'dagflow_stress_bench', '-j', args.jobs], 'build-' + label)
            binary = build / 'dagflow-stress-bench'
            binaries[profile, variant] = binary
            manifest['binaries'][label] = dict(path=str(binary), sha256=digest(binary), flags=flags)
            save(out / 'manifest.json', manifest)
            print('Built ' + label, flush=True)

    def command_for(binary, case, min_ms=0):
        # Wake latency needs a fixed number of independent sleeps. An active
        # duration target can otherwise turn microsecond bursts into hours of
        # inter-burst sleeping; main itself still supports that explicit mode.
        if case['scenario'] == 'idle-burst':
            min_ms = 0
        command = [binary, '--json', '--warmup', args.warmup, '--warmup-ms', args.warmup_ms,
                   '--repeats', args.repeats, '--bursts', args.bursts, '--min-ms', min_ms,
                   '--verify', args.verify, '--sample-stride', args.sample_stride]
        if args.latency:
            command += ['--latency']
        for key, value in case.items():
            if value is not None:
                command += ['--' + key.replace('_', '-'), value]
        return command

    rows, timings = [], {}
    jobs = list(itertools.product(range(args.rounds), range(len(cases)), profiles))
    random.Random(args.seed).shuffle(jobs)
    for repeat, index, profile in jobs:
        label = f'timing-c{index}-{profile}-r{repeat}'
        _, output = runner.command(command_for(binaries[profile, 'baseline'], cases[index], args.min_ms), label)
        result = checked_output(output)
        timings.setdefault((index, profile), []).append(result)
        save(out / (label + '.json'), result)
        print(f'{label}: {result["run_p50_us"]:.2f} us', flush=True)

    available = []
    if args.perf != 'off' and shutil.which('perf'):
        for event_index, event in enumerate(dict.fromkeys(itertools.chain.from_iterable(event_groups))):
            label = f'probe-{event_index}'
            path = out / (label + '.csv')
            code, _ = runner.command(['perf', 'stat', '-x', ';', '-o', path, '-e', event, '--', 'true'], label, required=False)
            values = parse_stat(path) if path.exists() else {}
            if code == 0 and any(v['status'] == 'ok' for v in values.values()):
                available.append(event)
            else:
                warnings.append(f'{event} unavailable (see logs/{label}.stderr)')
    elif args.perf != 'off':
        warnings.append('perf executable unavailable; counters and profiles not collected')
    manifest['available_events'] = available
    save(out / 'manifest.json', manifest)
    for index, case in enumerate(cases):
        for profile in profiles:
            label = f'c{index}-{profile}'
            runs = timings[index, profile]
            medians = [r['run_p50_us'] for r in runs]
            row = dict(case=index, profile=profile, scenario=case['scenario'], workers=case['workers'],
                       producers=runs[0]['producers'], logical_tasks=runs[0]['logical_tasks'],
                       process_runs=len(runs), median_us=statistics.median(medians),
                       min_process_median_us=min(medians), max_process_median_us=max(medians))
            row['ns_per_task'] = row['median_us'] * 1000 / row['logical_tasks']
            if not args.no_diagnostics:
                _, output = runner.command(command_for(binaries[profile, 'diagnostics'], case), 'diagnostics-' + label)
                result = checked_output(output)
                save(out / ('diagnostics-' + label + '.json'), result)
                if result['diagnostic_overflow_threads']:
                    warnings.append(f'{label}: diagnostic threads exceeded private lanes; overflow={result["diagnostic_overflow_threads"]}')
                denominator = result['logical_tasks'] * result['sample_count']
                row.update({'diag_' + k + '_per_task': v / denominator for k, v in result['counts'].items()})
            for group_index, group in enumerate(event_groups):
                events = [e for e in group if e in available]
                if not events:
                    continue
                stat_label = f'stat-{label}-g{group_index}'
                path = out / (stat_label + '.csv')
                _, output = runner.command(['perf', 'stat', '-x', ';', '-o', path, '-e', ','.join(events), '--',
                                            *command_for(binaries[profile, 'baseline'], case)], stat_label, gated=True)
                result = checked_output(output)
                counters = parse_stat(path)
                save(out / (stat_label + '.json'), dict(harness=result, counters=counters))
                denominator = result['logical_tasks'] * result['sample_count']
                for event, counter in counters.items():
                    row[event + '_per_task'] = None if counter['value'] is None else counter['value'] / denominator
                    row[event + '_running_pct'] = counter['running_percent']
                    if counter['status'] != 'ok' or (counter['running_percent'] is not None and counter['running_percent'] < 90):
                        warnings.append(f'{stat_label}: {event}: {counter}')
            if args.perf == 'all' and 'cycles:u' in available:
                directory = out / ('stacks-' + label)
                directory.mkdir()
                data = directory / 'perf.data'
                # DWARF preserves the exact timing binary; recording is a separate
                # run. Truncated/unresolved stacks remain visible in artifacts.
                sampling = ['-c', args.idle_sample_period] if case['scenario'] == 'idle-burst' else ['-F', args.frequency]
                _, output = runner.command(['perf', 'record', '-o', data, '-e', 'cycles:u', *sampling,
                                            '--call-graph', 'dwarf,16384', '--',
                                            *command_for(binaries[profile, 'baseline'], case, args.profile_ms)],
                                           'record-' + label, gated=True)
                save(directory / 'harness.json', checked_output(output))
                for kind, flags in (('hotspots', ['--no-children', '--sort', 'symbol,dso']),
                                    ('callgraph', ['--children', '--call-graph', 'graph,0.5,caller'])):
                    _, report = runner.command(['perf', 'report', '-i', data, '--stdio', '--percent-limit', '0.5', *flags], kind + '-' + label)
                    (directory / (kind + '.txt')).write_text(report)
                code, _ = runner.command(['perf', 'script', '-i', data, '-s', source / 'scripts/perf_flamegraph.py',
                                           '--', directory / 'flamegraph.json'], 'flame-' + label, required=False)
                if code == 0:
                    from render_main_profile import flamegraph
                    tree = json.loads((directory / 'flamegraph.json').read_text())
                    if tree['metadata']['samples']:
                        flamegraph(tree, label + ' / ' + case['scenario'], directory / 'flamegraph.svg')
                        missing = tree['metadata']['missing_callchain_samples']
                        if missing:
                            warnings.append(f'{label}: {missing}/{tree["metadata"]["samples"]} stack samples have no callchain; leaf samples remain visible')
                    else:
                        warnings.append(f'{label}: no stack samples; increase --profile-ms')
                else:
                    warnings.append(f'{label}: perf Python scripting unavailable; see callgraph.txt and perf.data')
            rows.append(row)
            save(out / 'summary.json', rows)
            save(out / 'manifest.json', manifest)
            print('Collected ' + label, flush=True)
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with (out / 'summary.csv').open('w') as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    report = ['# Main harness measurements', '',
              'Timing = median of independent process medians. Diagnostics and perf runs are separate.',
              'Hardware events count user space. Counter CSV retains multiplexing percentages and unsupported events.',
              'See `manifest.json` for exact case parameters, CPU masks, source/binary hashes and warnings.', '',
              '| Case | Profile | Scenario | Workers | Producers | us/run | ns/task | Process median range, us |',
              '|---|---|---|---:|---:|---:|---:|---:|']
    for row in rows:
        report.append(f'| {row["case"]} | {row["profile"]} | {row["scenario"]} | {row["workers"]} | {row["producers"]} | '
                      f'{row["median_us"]:.2f} | {row["ns_per_task"]:.2f} | '
                      f'{row["min_process_median_us"]:.2f}–{row["max_process_median_us"]:.2f} |')
    report += ['', 'All counters and normalized diagnostic paths: [summary.csv](summary.csv).', '']
    report += ['| Case | Profile | cycles/task | instructions/task | branches/task | branch misses/task | cache refs/task | cache misses/task | faults/task |',
               '|---|---|---:|---:|---:|---:|---:|---:|---:|']
    for row in rows:
        values = [row.get(e + '_per_task') for e in (
            'cycles:u', 'instructions:u', 'branches:u', 'branch-misses:u',
            'cache-references:u', 'cache-misses:u', 'page-faults')]
        report.append(f'| {row["case"]} | {row["profile"]} | ' +
                      ' | '.join('unavailable' if v is None else f'{v:.3f}' for v in values) + ' |')
    report += ['']
    for svg in sorted(out.glob('stacks-*/flamegraph.svg')):
        report.append(f'- [{svg.parent.name}]({svg.relative_to(out)}): [callgraph]({svg.parent.name}/callgraph.txt)')
    report += ['', 'Warnings:', *['- ' + warning for warning in warnings]]
    (out / 'report.md').write_text('\n'.join(report) + '\n')
    manifest['finished_unix'] = time.time()
    save(out / 'manifest.json', manifest)
    print(out / 'report.md', flush=True)


if __name__ == '__main__':
    main()
