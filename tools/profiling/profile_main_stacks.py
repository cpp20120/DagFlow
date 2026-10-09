#!/usr/bin/env python3
"""Supplement main's exact Release profiles with diagnostic frame-pointer builds."""
import argparse
import itertools
import json
import os
from pathlib import Path

import shutil
import subprocess
import time

from profile_main import ROOT, SCENARIOS, digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    out = args.directory.resolve()
    manifest = json.loads((out / 'manifest.json').read_text())
    summary = json.loads((out / 'summary.json').read_text())
    source = out / 'source'
    for name, checksum in manifest['source_sha256'].items():
        if digest(source / name) != checksum:
            raise RuntimeError('Source snapshot changed: ' + name)
    commands = []
    env = dict(os.environ, LC_ALL='C', DEBUGINFOD_URLS='')
    cpus = {int(c) for c in manifest['cpus'].split(',')}
    os.sched_setaffinity(0, cpus)

    def command(argv, label):
        argv = list(map(str, argv))
        entry = dict(argv=argv, started_unix=time.time(), label=label)
        commands.append(entry)
        try:
            result = subprocess.run(argv, capture_output=True, text=True, env=env, timeout=300)
            (out / 'logs' / (label + '.stdout')).write_text(result.stdout)
            (out / 'logs' / (label + '.stderr')).write_text(result.stderr)
            entry['returncode'] = result.returncode
            if result.returncode:
                raise RuntimeError(result.stderr + result.stdout)
            return result.stdout
        finally:
            entry['elapsed_seconds'] = time.time() - entry['started_unix']
            (out / 'fp-commands.json').write_text(json.dumps(commands, indent=2) + '\n')

    binaries = {}
    for profile, lto in (('o3', 'none'), ('o3-lto', 'full')):
        build = out / 'build' / (profile + '-fp')
        command(['cmake', '-S', source, '-B', build, '-G', 'Ninja',
                 '-DCMAKE_CXX_COMPILER=clang++', '-DCMAKE_BUILD_TYPE=Release',
                 '-DCMAKE_CXX_FLAGS_RELEASE=-O3 -g -DNDEBUG -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer',
                 '-DDAGFLOW_BUILD_SHARED=OFF', '-DDAGFLOW_BUILD_STATIC=ON', '-DDAGFLOW_INSTALL=OFF',
                 '-DDAGFLOW_BUILD_EXAMPLES=OFF', '-DDAGFLOW_BUILD_STRESS_BENCH=ON', '-DDAGFLOW_BUILD_TESTS=OFF',
                 '-DDAGFLOW_USE_LLD=ON', '-DDAGFLOW_ENABLE_NATIVE=OFF',
                 '-DDAGFLOW_PGO_MODE=none', '-DDAGFLOW_ALLOCATOR=mimalloc',
                 '-DDAGFLOW_LTO_MODE=' + lto], 'fp-configure-' + profile)
        command(['cmake', '--build', build, '--target', 'dagflow_stress_bench', '-j', 4], 'fp-build-' + profile)
        binaries[profile] = build / 'dagflow-stress-bench'
    handler = out / 'perf_flamegraph.py'
    shutil.copy2(ROOT / 'tools/profiling/perf_flamegraph.py', handler)
    for profile, workers, scenario in itertools.product(('o3', 'o3-lto'), (1, 4), SCENARIOS):
        label = f'{profile}-{workers}-{scenario}'
        directory = out / 'profiles-fp' / label
        directory.mkdir(parents=True, exist_ok=True)
        row = next(r for r in summary if (r['profile'], r['workers'], r['scenario']) ==
                   (profile, workers, scenario))
        period = max(10000, int(row['cycles:u'] / 1500))
        data = directory / 'perf.data'
        command(['perf', 'record', '-o', data, '-e', 'cycles:u', '-c', period, '--call-graph', 'fp',
                 '--', binaries[profile], '--workers', workers, '--scenario', scenario], 'fp-record-' + label)
        for kind, flags in (('hotspots', ['--no-children', '--sort', 'symbol,dso']),
                            ('callgraph', ['--children', '--call-graph', 'graph,0.5,caller'])):
            result = command(['perf', 'report', '-i', data, '--stdio', '--percent-limit', '0.5',
                              *flags], 'fp-' + kind + '-' + label)
            (directory / (kind + '.txt')).write_text(result)
        command(['perf', 'script', '-i', data, '-s', handler, '--', directory / 'flamegraph.json'],
                'fp-flame-' + label)
        print('FP profile: ' + label, flush=True)
    manifest['diagnostic_frame_pointers'] = dict(
        flags='-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer',
        call_graph='fp', cpus=manifest['cpus'],
        purpose='supplemental stacks only; excluded from primary timing/counter tables',
        binaries={k: dict(path=str(v), sha256=digest(v)) for k, v in binaries.items()})
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
