#!/usr/bin/env python3
"""Workload accounting, verification, timestamps and perf phase boundaries."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

BINARY = sys.argv.pop(1)
RUNNER = Path(__file__).resolve().parents[1] / 'scripts/benchmark_main.py'
spec = importlib.util.spec_from_file_location('benchmark_main', RUNNER)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
BASE = [BINARY, '--json', '--tasks', '37', '--warmup', '1', '--warmup-ms', '0',
        '--repeats', '2', '--bursts', '2', '--idle-us', '100', '--verify', 'exact']


def run(*arguments):
    result = subprocess.run([*BASE, *map(str, arguments)], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise AssertionError(result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines()]


class HarnessTests(unittest.TestCase):
    def check_result(self, result):
        self.assertEqual(result['status'], 'ok')
        self.assertTrue(result['duration_satisfied'])
        self.assertEqual(result['sample_count'], len(result['samples']))
        self.assertAlmostEqual(result['measured_us'], sum(s['elapsed_us'] for s in result['samples']), places=4)
        for sample in result['samples']:
            self.assertGreater(sample['elapsed_us'], 0)
            self.assertGreaterEqual(sample['drain_tail_us'], 0)
            self.assertLessEqual(sample['last_finish_us'], sample['elapsed_us'])
            self.assertEqual(len(sample['start_latency_us']), len(sample['finish_latency_us']))
            for start, finish in zip(sample['start_latency_us'], sample['finish_latency_us']):
                self.assertGreaterEqual(start, 0)
                self.assertGreaterEqual(finish, start)
            if result['diagnostics']:
                self.assertEqual(sample['counts']['packets'], result['logical_tasks'])
                self.assertEqual(sample['counts']['executed'], result['logical_tasks'])
                self.assertEqual(sample['counts']['external_submit'] + sample['counts']['worker_submit'], result['logical_tasks'])

    def test_all_scenarios_and_worker_counts(self):
        outputs = []
        for workers in (1, 4):
            results = run('--scenario', 'all', '--workers', workers, '--producers', 3, '--latency', '--sample-stride', 1)
            self.assertEqual(len(results), 7)
            for result in results:
                self.check_result(result)
                expected = {'local-overflow': 38, 'nested-helping': 74, 'mixed-chaos': 60}.get(result['scenario'], 37)
                self.assertEqual(result['logical_tasks'], expected)
                self.assertEqual(result['latency_sample_count'], 2 * expected)
            outputs.append({r['scenario']: r['checksum'] for r in results})
        self.assertEqual(*outputs)

    def test_batch_and_producer_lifetime(self):
        outputs = []
        for batch in (0, 1, 16, 64):
            for mode in ('persistent', 'fresh'):
                result, = run('--scenario', 'external-contention', '--workers', 2, '--producers', 3,
                              '--submit-batch', batch, '--producer-mode', mode, '--placement', 'none')
                self.check_result(result)
                outputs.append(result['checksum'])
        self.assertEqual(len(set(outputs)), 1)

    def test_zero_fanout_and_native_counts(self):
        result, = run('--scenario', 'mixed-chaos', '--workers', 2, '--fanout', 0, '--submit-batch', 32)
        self.check_result(result)
        self.assertEqual(result['logical_tasks'], 40)
        self.assertEqual(result['submit_batch'], 0)

    def test_active_duration_and_cap(self):
        result, = run('--scenario', 'idle-burst', '--workers', 1, '--idle-us', 10000,
                      '--min-ms', 1, '--max-samples', 1000)
        self.assertGreaterEqual(result['measured_us'], 1000)
        result, = run('--scenario', 'idle-burst', '--workers', 1, '--min-ms', 60000,
                      '--max-samples', 2)
        self.assertFalse(result['duration_satisfied'])

    def test_invalid_options(self):
        for options in (['--workers', '0'], ['--producers', '0'], ['--tasks', '-1'],
                        ['--submit-batch', '65'], ['--verify', 'yes'],
                        ['--perf-control-fd', '1'], ['--worker-cpus', '0,0']):
            result = subprocess.run([*BASE, *options], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2, result.stdout)

    @unittest.skipUnless(sys.platform == 'linux', 'Linux perf control protocol')
    def test_perf_ack_framing(self):
        for ack in (b'ack\n', b'ack\n\0'):
            cr, cw = os.pipe()
            ar, aw = os.pipe()
            commands = []
            def controller():
                with os.fdopen(cr, 'rb') as reader, os.fdopen(aw, 'wb', buffering=0) as writer:
                    for command in reader:
                        commands.append(command)
                        writer.write(ack)
            thread = threading.Thread(target=controller)
            thread.start()
            try:
                result = subprocess.run([*BASE, '--workers', '1', '--scenario', 'external-contention',
                                         '--warmup', '3', '--perf-control-fd', str(cw), '--perf-ack-fd', str(ar)],
                                        pass_fds=(cw, ar), capture_output=True, text=True, timeout=15)
            finally:
                os.close(cw)
                os.close(ar)
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(commands, [b'enable\n', b'disable\n'] * 2)
            self.assertTrue(json.loads(result.stdout)['perf_gated'])

    def test_overflow_accounting(self):
        result, = run('--scenario', 'local-overflow', '--workers', 1, '--tasks', 65536,
                      '--warmup', 0, '--repeats', 1)
        self.check_result(result)
        if result['diagnostics']:
            self.assertEqual(result['counts']['inline_execute'], 0)
            self.assertEqual(result['counts']['overflow_push'], 48128)
            self.assertEqual(result['counts']['overflow_acquire'], 48128)
            self.assertEqual(result['counts']['local_push'], 1024)
            self.assertEqual(result['counts']['cross_thread_free'], 1)

    def test_stat_unavailable_is_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'stat.csv'
            path.write_text('0;;page-faults;100;100.00;;\n<not supported>;;cycles:u;0;0.00;;\n')
            values = runner.parse_stat(path)
            self.assertEqual(values['page-faults']['value'], 0)
            self.assertIsNone(values['cycles:u']['value'])
            self.assertEqual(values['cycles:u']['status'], '<not supported>')


if __name__ == '__main__':
    unittest.main()
