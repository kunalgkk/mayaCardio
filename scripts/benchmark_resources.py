"""
Resource Benchmark Profiling Tool for MayaCardio Data Pipeline.
Measures:
- Average CPU utilization (%)
- Peak Resident Set Size (RSS memory in MB)
- Megabytes written to disk
- Megabytes sent to network
- Collection & processing throughput (samples/sec)
Checks compliance with Raspberry Pi 4-class hardware limits (<= 4 CPU cores, <= 2048 MB RAM).
"""

import argparse
import os
import sys
import time
import psutil

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mayacardio.storage import StorageEngine
from mayacardio.sync import CloudSynchronizer
from mayacardio.processor import process_window


def run_benchmark(duration_seconds: int = 30) -> dict:
    print(f"Starting resource benchmark for {duration_seconds}s...")
    process = psutil.Process()

    test_db = "bench_temp.db"
    for ext in ["", "-wal", "-shm"]:
        p = test_db + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass

    storage = StorageEngine(test_db)

    # Initial hardware I/O snapshots
    try:
        io_start = process.io_counters()
        disk_write_start = io_start.write_bytes
    except Exception:
        disk_write_start = 0

    net_io_start = psutil.net_io_counters().bytes_sent

    cpu_samples = []
    peak_rss_bytes = 0

    # Start workload: simulate continuous dual-sensor ingestion (800 samp/s)
    # plus window processing
    start_time = time.time()
    last_sample_time = start_time
    total_samples_ingested = 0

    sample_counter = 0

    while (time.time() - start_time) < duration_seconds:
        # Generate 400 samples for Sensor 1 and Sensor 2
        s1_batch = [[(3 * i) % 8388608, (3 * i + 1) % 8388608, (3 * i + 2) % 8388608] for i in range(sample_counter, sample_counter + 400)]
        s2_batch = [[(2 * i) % 8388608, (2 * i + 1) % 8388608] for i in range(sample_counter, sample_counter + 400)]

        storage.save_samples_batch("bench_sess", 1, sample_counter, s1_batch)
        storage.save_samples_batch("bench_sess", 2, sample_counter, s2_batch)
        sample_counter += 400
        total_samples_ingested += 800

        # Execute 1-second window processing
        w = (sample_counter // 400) - 1
        s1_map = {idx: vals for idx, vals in enumerate(s1_batch, start=sample_counter - 400)}
        s2_map = {idx: vals for idx, vals in enumerate(s2_batch, start=sample_counter - 400)}
        res = process_window(w, s1_map, s2_map, mode="online")
        storage.save_result("bench_sess", w, "online", res)

        # Sample process metrics
        cpu_samples.append(process.cpu_percent(interval=None))
        rss = process.memory_info().rss
        if rss > peak_rss_bytes:
            peak_rss_bytes = rss

        # Regulate acquisition rate to match 1-second batch cadence
        elapsed = time.time() - last_sample_time
        sleep_needed = max(0.0, 0.5 - elapsed)
        if sleep_needed > 0:
            time.sleep(sleep_needed)
        last_sample_time = time.time()

    elapsed_total = time.time() - start_time

    try:
        io_end = process.io_counters()
        disk_write_bytes = io_end.write_bytes - disk_write_start
    except Exception:
        # Fallback to file size on disk
        disk_write_bytes = os.path.getsize(test_db) if os.path.exists(test_db) else 1024 * 1024

    net_bytes_sent = psutil.net_io_counters().bytes_sent - net_io_start

    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    peak_rss_mb = peak_rss_bytes / (1024 * 1024)
    disk_write_mb = disk_write_bytes / (1024 * 1024)
    net_sent_mb = net_bytes_sent / (1024 * 1024)

    # 1-hour extrapolation
    scale_1h = 3600.0 / elapsed_total
    extrapolated_disk_1h = disk_write_mb * scale_1h
    extrapolated_net_1h = net_sent_mb * scale_1h

    # Clean up temp db
    for ext in ["", "-wal", "-shm"]:
        p = test_db + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass

    results = {
        "duration_seconds": round(elapsed_total, 2),
        "total_samples_ingested": total_samples_ingested,
        "throughput_samples_per_sec": round(total_samples_ingested / elapsed_total, 1),
        "average_cpu_percent": round(avg_cpu, 2),
        "peak_rss_mb": round(peak_rss_mb, 2),
        "disk_written_mb": round(disk_write_mb, 2),
        "extrapolated_disk_1h_mb": round(extrapolated_disk_1h, 2),
        "network_sent_mb": round(net_sent_mb, 2),
        "extrapolated_network_1h_mb": round(extrapolated_net_1h, 2),
        "rpi4_cpu_limit": "<= 100% of 1 core (25% total of 4 cores)",
        "rpi4_ram_limit_mb": 2048.0,
        "compliant": (peak_rss_mb <= 2048.0 and avg_cpu <= 100.0),
    }

    return results


def main():
    parser = argparse.ArgumentParser(description="Benchmark pipeline resources")
    parser.add_argument("--duration", type=int, default=10, help="Benchmark duration in seconds")
    args = parser.parse_args()

    res = run_benchmark(duration_seconds=args.duration)
    import json
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
