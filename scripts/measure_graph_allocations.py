#!/usr/bin/env python3
"""Record graph runtime-boundary requests using a diagnostics-enabled suite."""
import argparse
import json
from pathlib import Path

from benchmark_main import Runner, digest, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    binary, out = args.binary.resolve(), args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    runner = Runner(out, 60)
    save(out / 'manifest.json', dict(binary=str(binary), sha256=digest(binary),
         scope='successful runtime_memory requests; no backend size-class rounding',
         warmup=3, repeats=10, tasks=4096, iterations=0))
    summaries = []
    for workers in (1, 4):
        for scenario in ('deep_dag', 'fanout_fanin', 'graph_reuse', 'dag_build_run',
                         'graph_tokens_serial', 'graph_tokens_parallel'):
            label = f'{scenario}-{workers}'
            argv = [binary, '--scenario', scenario, '--workers', workers,
                    '--tasks', 4096, '--iterations', 0, '--warmup', 3, '--repeats', 10]
            _, output = runner.command(argv, label)
            result = json.loads(output)
            if result['status'] != 'ok' or not result.get('diagnostics'):
                raise RuntimeError('an instrumented runtime suite is required')
            save(out / (label + '.json'), result)
            counts = result['diagnostics']
            summaries.append(dict(scenario=scenario, workers=workers,
                per_run={k: counts[k] / result['repeats'] for k in (
                    'memory_allocations', 'memory_requested_bytes', 'memory_deallocations',
                    'packet_allocations', 'packet_allocated_bytes', 'completion_created',
                    'graph_run_state_allocations', 'graph_continuations')}))
    save(out / 'summary.json', summaries)
    print(out / 'summary.json')


if __name__ == '__main__':
    main()
