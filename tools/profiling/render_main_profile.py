#!/usr/bin/env python3
"""Render perf's flamegraph JSON as offline, zoomable SVGs and an HTML index."""
import argparse
from collections import Counter
import hashlib
from html import escape
import json
from pathlib import Path


def flamegraph(tree, title, path):
    frames = []
    leaves = Counter()

    def total(node):
        node['_total'] = node['v'] + sum(total(c) for c in node['c'])
        leaves[node['n']] += node['v']
        return node['_total']

    count = total(tree)
    if count == 0:
        raise ValueError(f'No samples in {path}')

    def layout(node, x, depth):
        frames.append((node, x, depth))
        position = x
        for child in sorted(node['c'], key=lambda n: n['n']):
            layout(child, position, depth + 1)
            position += child['_total']

    layout(tree, 0, 0)
    depth = max(d for _, _, d in frames)
    width, margin, step = 1400, 10, 19
    height = 110 + (depth + 1) * step
    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<style>text {{font-family:monospace;font-size:11px;pointer-events:none}} .frame {{cursor:pointer}} .frame:hover rect {{stroke:black;stroke-width:1}} .control {{cursor:pointer;font-family:sans-serif}}</style>
<rect width="100%" height="100%" fill="#fafafa"/>
<text x="10" y="24" style="font-size:16px">{escape(title)}</text>
<text x="10" y="45">Width = sampled cycles (sum of periods: {count:,}); root at bottom. Click a frame to zoom.</text>
<g class="control" onclick="reset()"><rect x="10" y="55" width="90" height="24" fill="#ddd"/><text x="18" y="72">Reset zoom</text></g>
<g class="control" onclick="search()"><rect x="110" y="55" width="75" height="24" fill="#ddd"/><text x="120" y="72">Search</text></g>''']
    for node, x, d in frames:
        # Preserve tiny frames in the DOM so zoom can reveal them.
        y = height - 15 - (d + 1) * step
        w = node['_total'] / count * (width - 2 * margin)
        px = margin + x / count * (width - 2 * margin)
        hue = int(hashlib.sha256(node['n'].encode()).hexdigest()[:8], 16) % 55
        color = f'hsl({hue},85%,70%)'
        label = node['n'][:max(0, int(w / 7) - 1)] if w >= 14 else ''
        tooltip = f"{node['n']} — {node['_total']:,} sampled cycles ({100 * node['_total'] / count:.2f}%), self {node['v']:,}"
        parts.append(f'<g class="frame" data-x="{x}" data-w="{node["_total"]}" data-d="{d}" data-name="{escape(node["n"], quote=True)}" onclick="zoom(this)"><title>{escape(tooltip)}</title><rect x="{px:.3f}" y="{y}" width="{w:.3f}" height="18" fill="{color}"/><text x="{px + 3:.3f}" y="{y + 13}">{escape(label)}</text></g>')
    parts.append('''<script><![CDATA[
const full = COUNT, left = 10, span = 1380;
function draw(x, w, depth) {
  document.querySelectorAll('.frame').forEach(g => {
    const gx = +g.dataset.x, gw = +g.dataset.w, gd = +g.dataset.d;
    const ancestor = gd < depth && gx <= x && gx + gw >= x + w;
    const child = gd >= depth && gx >= x && gx + gw <= x + w;
    g.style.display = ancestor || child ? '' : 'none';
    if (!ancestor && !child) return;
    const px = ancestor ? left : left + (gx - x) / w * span;
    const pw = ancestor ? span : gw / w * span;
    const r = g.querySelector('rect'), t = g.querySelector('text');
    r.setAttribute('x', px); r.setAttribute('width', pw);
    t.setAttribute('x', px + 3);
    t.textContent = pw < 14 ? '' : g.dataset.name.slice(0, Math.max(0, Math.floor(pw / 7) - 1));
  });
}
function zoom(g) { draw(+g.dataset.x, +g.dataset.w, +g.dataset.d); }
function reset() { draw(0, full, 0); }
function search() {
  const term = prompt('Highlight function name (empty to clear):');
  if (term === null) return;
  document.querySelectorAll('.frame').forEach(g => {
    const hit = term && g.dataset.name.toLowerCase().includes(term.toLowerCase());
    g.querySelector('rect').style.stroke = hit ? '#6000ee' : '';
    g.querySelector('rect').style.strokeWidth = hit ? '2' : '';
  });
}
]]></script></svg>'''.replace('COUNT', str(count)))
    path.write_text('\n'.join(parts) + '\n')
    return dict(samples=tree['metadata']['samples'], sampled_cycles=count,
                missing_callchain_samples=tree['metadata']['missing_callchain_samples'], max_depth=depth,
                top_self=[dict(symbol=symbol, sampled_cycles=n, pct=100*n/count)
                          for symbol, n in leaves.most_common(15)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    out = args.directory
    summary = json.loads((out / 'summary.json').read_text())
    profiles = {}
    for row in summary:
        label = f"{row['profile']}-{row['workers']}-{row['scenario']}"
        directory = out / 'profiles' / label
        tree = json.loads((directory / 'flamegraph.json').read_text())
        profiles[label] = flamegraph(tree, label, directory / 'flamegraph.svg')
    (out / 'sampling-summary.json').write_text(json.dumps(profiles, indent=2) + '\n')
    fp_profiles = {}
    for label in profiles:
        directory = out / 'profiles-fp' / label
        if (directory / 'flamegraph.json').exists():
            tree = json.loads((directory / 'flamegraph.json').read_text())
            fp_profiles[label] = flamegraph(tree, label + ' (diagnostic frame pointers)',
                                           directory / 'flamegraph.svg')
    (out / 'sampling-summary-fp.json').write_text(json.dumps(fp_profiles, indent=2) + '\n')
    body = ['''<!doctype html><html lang="en"><meta charset="utf-8"><title>DagFlow main.cpp: O3 / Full LTO</title>
<style>body{font:15px system-ui;margin:2rem;color:#19212c}table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:.5rem;text-align:right}td:first-child,td:nth-child(3){text-align:left}a{color:#145eb0}thead{background:#eee}</style>
<h1>bench/stress_harness.cpp: O3 versus O3 + Full LTO</h1>
<p>Same workload source, mimalloc, 1/4 workers. Timing: median of five independent process medians, without perf.
Counters: median of three whole-process perf stat runs; include startup and warmups. Hardware events are userspace only.
Flamegraphs: separate cycles:u sampling runs, widths proportional to sample periods, not elapsed time or exact cycle totals.
Idle-burst time is burst completion p50, not per-task start latency. CPU frequency and host load were not controlled.</p>
<p>Release DWARF unwinding is incomplete in some cases. Diagnostic FP builds add frame pointers to the same source for clearer caller stacks;
their timings and counters are excluded from this table. Unresolved frames remain visible. Neither profile includes off-CPU waiting time.</p>
<p><a href="summary.csv">All counters (CSV)</a> · <a href="manifest.json">Build manifest</a> · <a href="README.md">Report</a></p>
<table><thead><tr><th>Build</th><th>Workers</th><th>Scenario</th><th>Time, µs</th><th>Cycles, M</th><th>Instructions, M</th><th>Branches, M</th><th>Branch misses, M</th><th>Cache refs, M</th><th>Cache misses, M</th><th>Page faults</th><th>Samples</th><th>Profiles</th></tr></thead><tbody>''']
    for row in summary:
        label = f"{row['profile']}-{row['workers']}-{row['scenario']}"
        link = 'profiles/' + label
        cells = [escape(row['profile']), str(row['workers']), escape(row['scenario']), f"{row['time_us']:.2f}"]
        cells += [f"{row[e]/1e6:.3f}" for e in ('cycles:u','instructions:u','branches:u','branch-misses:u','cache-references:u','cache-misses:u')]
        cells += [f"{row['page-faults']:.0f}", str(profiles[label]['samples']),
                  f'<a href="{link}/flamegraph.svg">Release flamegraph</a> · <a href="{link}/callgraph.txt">Release call graph</a> · <a href="{link}/hotspots.txt">Hotspots</a>']
        if label in fp_profiles:
            fp_link = 'profiles-fp/' + label
            cells[-1] += f'<br><a href="{fp_link}/flamegraph.svg">FP flamegraph</a> · <a href="{fp_link}/callgraph.txt">FP call graph</a>'
        body.append('<tr>' + ''.join('<td>' + c + '</td>' for c in cells) + '</tr>')
    body.append('</tbody></table><p>SVGs are self-contained; open directly to zoom and search. Raw perf.data and flamegraph.json are next to each SVG.</p></html>')
    (out / 'index.html').write_text('\n'.join(body) + '\n')
    report = out / 'README.md'
    if not report.exists():
        report.write_text('''# main.cpp: O3 versus Full LTO

Open [index.html](index.html) for all timings, counters and profile links.
[summary.csv](summary.csv) contains exact values; [manifest.json](manifest.json)
identifies the source, flags, binaries and sampling configuration.

Timings come from independent runs without perf. Counter medians cover the whole
process including startup and warmups. Recorded stacks come from separate runs;
SVG widths sum sample periods. FP profiles, if present, use diagnostic builds
with frame pointers and do not contribute to the timing/counter tables.

CPU frequency and host load were not fixed. Idle-burst measures completion of a
burst, not a single task's start latency. Unresolved symbols and missing callers
remain visible; CPU sampling does not measure off-CPU waiting time.
''')
    print(f'Rendered {len(profiles) + len(fp_profiles)} flamegraphs: {out / "index.html"}')


if __name__ == '__main__':
    main()
