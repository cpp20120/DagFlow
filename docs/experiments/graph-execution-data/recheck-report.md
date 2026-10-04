# Graph regression recheck

Exact original binary hashes and CPU masks; 15 paired processes per variant, 30 warmups and 201 measured repetitions per process.
No outlier removal. Positive change means slower. CI: percentile bootstrap 95% interval for the median paired percentage (10,000 resamples); exploratory, no multiple-comparison correction.

| Scenario | W | Tasks | Work iterations | Profile | Previous | Recheck | Paired median [95% CI] | Slower pairs | Baseline → candidate µs |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|
| deep_dag | 4 | 4096 | 0 | o3 | -35.9% | -35.1% | -34.7% [-35.9, -34.5] | 0/15 | 184.95 → 119.98 |
| deep_dag | 4 | 4096 | 0 | o3-lto | -34.8% | -34.7% | -34.7% [-35.0, -34.3] | 0/15 | 181.69 → 118.64 |
| deep_dag | 4 | 4096 | 128 | o3-lto | +10.9% | +3.1% | +3.4% [+1.7, +5.8] | 14/15 | 1472.23 → 1518.41 |
| dag_build_run | 1 | 4096 | 128 | o3 | +0.5% | +0.4% | +0.5% [-0.7, +1.5] | 9/15 | 1264.46 → 1269.74 |
| dag_build_run | 1 | 4096 | 128 | o3-lto | +2.2% | +0.2% | +0.6% [+0.2, +2.1] | 11/15 | 1261.54 → 1263.88 |
| graph_tokens_serial | 1 | 4096 | 0 | o3 | +4.3% | -0.1% | +0.3% [-0.3, +0.7] | 8/15 | 27.77 → 27.75 |
| graph_tokens_serial | 1 | 4096 | 128 | o3-lto | +0.9% | +0.6% | +0.1% [-2.0, +0.7] | 10/15 | 1026.00 → 1032.09 |
| graph_tokens_serial | 4 | 4096 | 128 | o3 | +1.1% | +4.1% | +4.8% [+0.1, +5.7] | 11/15 | 1187.33 → 1235.92 |
| graph_tokens_serial | 4 | 4096 | 128 | o3-lto | +7.8% | +1.7% | +2.9% [-1.4, +6.2] | 10/15 | 1213.30 → 1233.39 |
| graph_tokens_parallel | 1 | 4096 | 0 | o3 | +5.5% | -0.2% | -0.8% [-2.0, +0.1] | 5/15 | 27.81 → 27.76 |
| graph_tokens_parallel | 1 | 4096 | 128 | o3-lto | +0.0% | +0.5% | +0.4% [+0.2, +1.1] | 11/15 | 1027.77 → 1032.55 |
| graph_tokens_parallel | 4 | 4096 | 128 | o3-lto | +1.8% | -0.1% | -0.2% [-1.7, +2.1] | 6/15 | 371.25 → 371.04 |
| graph_tokens_serial | 1 | 65 | 0 | o3 | +12.3% | -0.2% | +0.0% [-0.8, +1.2] | 7/15 | 8.14 → 8.13 |
