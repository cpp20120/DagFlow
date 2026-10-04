# Graph regression recheck

Exact original binary hashes and CPU masks; 15 paired processes per variant, 30 warmups and 201 measured repetitions per process.
No outlier removal. Positive change means slower. CI: percentile bootstrap 95% interval for the median paired percentage (10,000 resamples); exploratory, no multiple-comparison correction.

| Scenario | W | Tasks | Work iterations | Profile | Previous | Recheck | Paired median [95% CI] | Slower pairs | Baseline → candidate µs |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|
| deep_dag | 1 | 4096 | 0 | o3 | +6.8% | -1.1% | -0.5% [-4.4, +0.4] | 4/15 | 49.30 → 48.74 |
| deep_dag | 1 | 4096 | 0 | o3-lto | +2.0% | -2.1% | -2.1% [-4.0, -1.1] | 2/15 | 48.97 → 47.96 |
| deep_dag | 1 | 4096 | 128 | o3 | +0.4% | +0.2% | +0.2% [-0.1, +0.8] | 9/15 | 1071.97 → 1073.60 |
| deep_dag | 4 | 4096 | 0 | o3 | -8.1% | -7.9% | -7.9% [-9.8, -6.6] | 0/15 | 121.33 → 111.77 |
| deep_dag | 4 | 4096 | 0 | o3-lto | -8.3% | -6.9% | -5.7% [-8.1, -4.8] | 0/15 | 119.93 → 111.61 |
| deep_dag | 4 | 4096 | 128 | o3-lto | +1.8% | +1.8% | +1.8% [-0.3, +6.5] | 11/15 | 1470.80 → 1497.66 |
| dag_build_run | 1 | 4096 | 0 | o3 | +6.6% | -0.4% | -1.2% [-1.4, +0.3] | 4/15 | 217.48 → 216.67 |
| dag_build_run | 1 | 4096 | 0 | o3-lto | +1.1% | +0.3% | +0.1% [-0.4, +1.1] | 8/15 | 215.36 → 215.99 |
| dag_build_run | 4 | 4096 | 0 | o3-lto | +0.0% | -0.5% | -0.2% [-2.2, +0.9] | 6/15 | 296.75 → 295.26 |
| dag_build_run | 4 | 4096 | 128 | o3 | +2.0% | +0.5% | +1.0% [-0.2, +1.6] | 11/15 | 1748.06 → 1756.81 |
| graph_tokens_serial | 1 | 4096 | 128 | o3 | +0.0% | -0.3% | -0.4% [-0.7, +0.3] | 6/15 | 1045.05 → 1041.69 |
| graph_tokens_serial | 1 | 4096 | 128 | o3-lto | +0.6% | +0.2% | +0.0% [-1.3, +0.7] | 8/15 | 1043.03 → 1044.68 |
| graph_tokens_serial | 4 | 4096 | 0 | o3 | +4.8% | +1.3% | +0.9% [+0.6, +2.4] | 12/15 | 78.87 → 79.92 |
| graph_tokens_serial | 4 | 4096 | 128 | o3 | +5.5% | +1.7% | +0.8% [-0.2, +3.8] | 10/15 | 1199.30 → 1220.18 |
| graph_tokens_serial | 4 | 4096 | 128 | o3-lto | +3.5% | +2.1% | +1.3% [+0.0, +4.5] | 11/15 | 1211.79 → 1237.51 |
| graph_tokens_parallel | 1 | 4096 | 0 | o3-lto | +0.2% | +0.2% | -0.1% [-1.2, +1.1] | 7/15 | 27.83 → 27.89 |
| graph_tokens_parallel | 1 | 4096 | 128 | o3-lto | +0.1% | -0.0% | -0.1% [-0.4, +0.2] | 5/15 | 1044.97 → 1044.53 |
| graph_tokens_parallel | 4 | 4096 | 0 | o3-lto | +0.5% | +1.8% | +1.1% [+0.3, +2.1] | 13/15 | 340.62 → 346.66 |
| graph_tokens_parallel | 4 | 4096 | 128 | o3 | +0.3% | -0.2% | -0.2% [-0.7, +0.2] | 5/15 | 306.36 → 305.61 |
| graph_tokens_parallel | 4 | 4096 | 128 | o3-lto | +1.2% | +0.2% | +0.1% [-2.7, +1.8] | 8/15 | 378.04 → 378.93 |
| deep_dag | 1 | 65 | 0 | o3 | +6.4% | -1.8% | -1.9% [-10.6, -0.5] | 2/15 | 8.54 → 8.38 |
| graph_tokens_serial | 1 | 64 | 0 | o3 | +49.5% | -1.8% | -0.5% [-7.1, +0.2] | 4/15 | 8.25 → 8.10 |
| graph_tokens_serial | 1 | 65 | 0 | o3-lto | +0.2% | -1.2% | -0.7% [-4.9, +29.4] | 7/15 | 8.28 → 8.18 |
