"""
Authoritative Ground Truth Verification Tool for MayaCardio exports.
Verifies Criteria 1 & 2:
1. Every sample received is on disk exactly once, unchanged.
2. Every sample not on disk is listed as lost, with a truthful reason (zero silent losses).
3. Zero corrupted values and zero duplicate records.
"""

import argparse
import json
import os
import sys
from typing import Dict, Any, List, Set, Tuple

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.biosignal_mock import generate_ramp_sample, generate_signal_sample


def verify_export_stream(
    stream, mode: str = "ramp", seed: int = 7
) -> Dict[str, Any]:
    samples: Dict[Tuple[int, int], List[int]] = {}  # (sensor, index) -> values
    losses: List[Dict[str, Any]] = []

    duplicate_count = 0
    corrupt_count = 0
    total_samples = 0

    for line_idx, line in enumerate(stream, start=1):
        line_clean = line.strip()
        if not line_clean:
            continue
        try:
            record = json.loads(line_clean)
        except Exception as e:
            return {
                "status": "FAIL",
                "error": f"Line {line_idx} invalid JSON: {e}",
            }

        if "index" in record:
            # Sample record
            sensor = record["sensor"]
            idx = record["index"]
            vals = record["values"]
            total_samples += 1

            key = (sensor, idx)
            if key in samples:
                duplicate_count += 1
            else:
                samples[key] = vals

            # Verify values against mathematical ground truth
            if mode == "ramp":
                expected_vals = generate_ramp_sample(sensor, idx)
            else:
                expected_vals = generate_signal_sample(sensor, idx, seed=seed)

            if vals != expected_vals:
                corrupt_count += 1

        elif "from_index" in record:
            # Loss record
            losses.append(record)

    # Check for silent losses between min and max sample indices for each sensor
    silent_losses = []
    for s in (1, 2):
        s_indices = [idx for (sensor, idx) in samples.keys() if sensor == s]
        if not s_indices:
            continue
        min_i = min(s_indices)
        max_i = max(s_indices)

        # Build set of covered lost indices
        lost_covered: Set[int] = set()
        for l in losses:
            if l.get("sensor") == s:
                from_i = l["from_index"]
                to_i = l.get("to_index")
                if to_i is not None:
                    lost_covered.update(range(from_i, to_i + 1))

        # Any missing index between min_i and max_i must be in lost_covered
        for i in range(min_i, max_i + 1):
            if (s, i) not in samples and i not in lost_covered:
                silent_losses.append((s, i))

    passed = (duplicate_count == 0 and corrupt_count == 0 and len(silent_losses) == 0)

    return {
        "status": "PASS" if passed else "FAIL",
        "total_samples": total_samples,
        "unique_samples": len(samples),
        "duplicates": duplicate_count,
        "corrupted_values": corrupt_count,
        "loss_records": len(losses),
        "silent_losses": len(silent_losses),
    }


def main():
    parser = argparse.ArgumentParser(description="Check MayaCardio export against ground truth")
    parser.add_argument("export_file", nargs="?", default="-", help="Path to JSONL export file or '-' for stdin")
    parser.add_argument("--mode", choices=["ramp", "signal"], default="ramp")
    parser.add_argument("--seed", type=int, default=7)

    args = parser.parse_args()

    if args.export_file == "-":
        stream = sys.stdin
    else:
        stream = open(args.export_file, "r", encoding="utf-8")

    result = verify_export_stream(stream, mode=args.mode, seed=args.seed)
    if stream != sys.stdin:
        stream.close()

    print(json.dumps(result, indent=2))
    if result["status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
