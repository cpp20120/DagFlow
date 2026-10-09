#!/usr/bin/env python3
"""perf script Python handler: period-weighted trees, preserving missing stacks."""
import json
from pathlib import Path
import sys

root = dict(n='all', v=0, c=[])
samples = 0
missing_callchain_samples = 0


def child(node, name):
    for item in node['c']:
        if item['n'] == name:
            return item
    item = dict(n=name, v=0, c=[])
    node['c'].append(item)
    return item


def frame_name(entry):
    name = entry.get('sym', {}).get('name')
    if name:
        return name
    dso = Path(entry.get('dso', '[unknown]')).name
    # Keep unresolved frames visible; never silently attribute them to callers.
    return f"[unknown in {dso}]"


def process_event(event):
    global samples, missing_callchain_samples
    samples += 1
    period = event.get('sample', {}).get('period', 1)
    node = child(root, event.get('comm', '[unknown process]'))
    chain = event.get('callchain', [])
    if chain:
        for entry in reversed(chain):
            node = child(node, frame_name(entry))
    else:
        missing_callchain_samples += 1
        node = child(node, event.get('symbol', '[missing callchain]'))
    node['v'] += period


def trace_end():
    root['metadata'] = dict(samples=samples, missing_callchain_samples=missing_callchain_samples,
                            weight='sample.period', event='cycles:u')
    Path(sys.argv[1]).write_text(json.dumps(root) + '\n')
