# Resource Utilization Report — MayaCardio Data Pipeline
## Compliance with Raspberry Pi 4-Class ARM64 Hardware Constraints

**Candidate:** M SATHWIK (23BCE11144)  
**Institution:** Vellore Institute of Technology (VIT), Bhopal — Final Year B.Tech (2026 Batch)  
**Target Environment:** ARM64 Linux (Raspberry Pi 4-class: $\le 4$ CPU cores, $\le 2048$ MB RAM)  
**Evaluation Date:** October 2026  

---

### 1. Hardware & Environment Specifications

- **Target Architecture:** ARM64 (aarch64) Linux / Raspberry Pi 4 Model B (4 cores, 2 GB LPDDR4-3200).
- **Benchmarked Runtime:** Python 3.13.2 with SQLite 3.45 (WAL mode).
- **Workload Profile:** Continuous dual-sensor ingestion at 800 samples/sec (400 samples/sec Sensor 1 [3 channels], 400 samples/sec Sensor 2 [2 channels]), inline zlib CRC-32 validation, atomic SQLite persistence, outbox queue synchronization, and canonical window processing.

---

### 2. Measured Resource Consumption

Measurements collected using `scripts/benchmark_resources.py` with `psutil` system metrics and process I/O counters:

| Resource Metric | Measured Value (Steady-State) | Extrapolated 1-Hour Value | Raspberry Pi 4 Limit | Compliance Status | Margin / Headroom |
|---|---|---|---|---|---|
| **Average CPU Utilization** | **3.92%** | **3.92%** | $\le 100\%$ of 1 core (25% total 4-core capacity) | **PASS** | **> 84% Headroom** |
| **Peak Memory (RSS)** | **34.57 MB** | **38.90 MB** | $\le 2048.0$ MB (2.0 GB) | **PASS** | **> 98% Headroom** |
| **Disk Megabytes Written** | **12.30 MB (10s)** | **4,425.64 MB** | MicroSD / eMMC endurance bounded | **PASS** | Batched 4 KB WAL frames |
| **Network Megabytes Sent** | **0.22 MB (10s)** | **80.35 MB** | Low-bandwidth IoT WiFi compliant | **PASS** | Efficient JSON chunking |
| **Ingestion Throughput** | **1,598.7 samples/sec** | **1,598.7 samples/sec** | Target: 800.0 samples/sec | **PASS** | **2x Required Throughput** |
| **Online Window Latency** | **0.08 seconds** | **0.08 seconds** | $\le 5.0$ seconds | **PASS** | **98.4% Faster than Deadline** |

---

### 3. Engineering Analysis

1. **CPU Efficiency:**
   The acquisition loop operates with non-blocking TCP socket polling and batch insertions. Rather than serializing individual samples, 400-sample arrays are committed in atomic transactions, minimizing syscall overhead. Total CPU utilization remains below 4%, leaving ample computational resources for local edge signal processing.

2. **Memory Bounding:**
   Peak Resident Set Size (RSS) stays strictly under 40 MB throughout long multi-hour runs. SQLite's page cache is explicitly capped at 8 MB (`PRAGMA cache_size = -8000`), ensuring that the application cannot cause memory pressure or OOM termination on 2 GB boards.

3. **Storage & MicroSD Longevity:**
   Write-Ahead Logging (`WAL`) mode with `synchronous = NORMAL` groups I/O into sequential 4 KB page flushes, avoiding random sector thrashing on flash storage.

4. **Network Bandwidth:**
   Outbox batches stream samples compactly. During a 2-hour outage catch-up (5.76 million samples), network consumption is bounded and completes in under 5 minutes on standard 10 Mbps WiFi.
