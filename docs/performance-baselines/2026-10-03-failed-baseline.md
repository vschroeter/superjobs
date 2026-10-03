# SuperJobs performance baseline (2026-10-03)

This attempt failed: the harness returned 1 and the manifest producer at concurrency 8 returned 4 after recording six failure-bucket observations. This is retained failure evidence, not a correctness-validated baseline. The measured implementation is `af5fcecafbf6cb7b11a5528c147b6be0c74a1bb3`.

Actual request ranges copied from the emitted producer records were 1041–1043 bytes for telemetry and 65547–65549 bytes for manifest. The original summary did not retain failure details, so worker failures and producer-read exceptions cannot yet be distinguished. No historical comparison exists.

- Profile: **baseline** (baseline eligible: False)
- Revision: `af5fcecafbf6cb7b11a5528c147b6be0c74a1bb3` (clean: True)
- Child Python (producer): 3.12.11 (main, Jun  4 2025, 17:41:36)
- NATS server: 2.15.0
- Storage medium (broker temp): ssd
- CPU: AMD Ryzen 9 9950X 16-Core Processor            
- Telemetry request bytes: 1041
- Manifest request bytes: 65547
- Measurement clock (legacy): `monotonic` (GetTickCount64(), resolution=0.015625) — samples used `time.monotonic()` before the 2026-10-03 perf_counter correction; six submit-latency medians at 0.0 reflect quantization, not zero cost. Do not ratio-compare this artifact against `perf_counter` baselines.

## Combinations

### telemetry @ concurrency 1 (in-flight 1)
- Throughput variation across samples: min=13.35/s max=14.45/s spread=1.10/s
- Sample 1: 14.45 jobs/s (fixed interval 20.00s, observed 20.016s); success=289, late=0, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=289): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=289): median=0.0620s p95=0.0780s
- Sample 2: 13.55 jobs/s (fixed interval 20.00s, observed 20.000s); success=271, late=0, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=271): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=271): median=0.0630s p95=0.0780s
- Sample 3: 13.35 jobs/s (fixed interval 20.00s, observed 20.000s); success=267, late=0, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=267): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=267): median=0.0630s p95=0.0780s

### telemetry @ concurrency 8 (in-flight 8)
- Throughput variation across samples: min=72.40/s max=91.05/s spread=18.65/s
- Sample 1: 91.05 jobs/s (fixed interval 20.00s, observed 20.015s); success=1821, late=0, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=1821): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=1821): median=0.0620s p95=0.0780s
- Sample 2: 76.55 jobs/s (fixed interval 20.00s, observed 20.000s); success=1531, late=8, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=1539): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=1539): median=0.0620s p95=0.0931s
- Sample 3: 72.40 jobs/s (fixed interval 20.00s, observed 20.000s); success=1448, late=0, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=1448): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=1448): median=0.0620s p95=0.1090s

### manifest @ concurrency 1 (in-flight 1)
- Throughput variation across samples: min=7.35/s max=7.55/s spread=0.20/s
- Sample 1: 7.35 jobs/s (fixed interval 20.00s, observed 20.016s); success=147, late=1, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=148): median=0.0160s p95=0.0470s
  - End-to-end (successful completions, n=148): median=0.1400s p95=0.1570s
- Sample 2: 7.55 jobs/s (fixed interval 20.00s, observed 20.016s); success=151, late=1, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=152): median=0.0160s p95=0.0470s
  - End-to-end (successful completions, n=152): median=0.1400s p95=0.1570s
- Sample 3: 7.55 jobs/s (fixed interval 20.00s, observed 20.000s); success=151, late=0, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=151): median=0.0150s p95=0.0470s
  - End-to-end (successful completions, n=151): median=0.1250s p95=0.1560s

### manifest @ concurrency 8 (in-flight 8)
- Throughput variation across samples: min=50.80/s max=60.10/s spread=9.30/s
- Sample 1: 60.10 jobs/s (fixed interval 20.00s, observed 20.000s); success=1202, late=4, failures=0, validation=0, incomplete=0
  - Submit latency (successful completions, n=1206): median=0.0000s p95=0.0310s
  - End-to-end (successful completions, n=1206): median=0.0940s p95=0.1250s
- Sample 2: 54.50 jobs/s (fixed interval 20.00s, observed 20.000s); success=1090, late=0, failures=5, validation=0, incomplete=0
  - Submit latency (successful completions, n=1090): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=1090): median=0.1090s p95=0.1560s
- Sample 3: 50.80 jobs/s (fixed interval 20.00s, observed 20.000s); success=1016, late=0, failures=1, validation=0, incomplete=0
  - Submit latency (successful completions, n=1016): median=0.0000s p95=0.0160s
  - End-to-end (successful completions, n=1016): median=0.1100s p95=0.1570s

## API friction

- Producer in-flight limiting uses an explicit asyncio semaphore; SuperJobs does not expose a submit concurrency knob.
- No-result telemetry jobs: awaiting outcome() is sufficient for success; calling result() is optional and raises on failure.
- Manifest throughput validation replays events() to assert three ordered application events plus system terminals.
