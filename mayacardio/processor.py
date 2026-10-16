"""
Canonical Signal Processing Engine for MayaCardio pipeline.
Strict implementation of Section 3 of the candidate assessment.
Ensures identical mathematical behavior in both online (cloud) and offline (device) execution.
"""

import math
from typing import Dict, Any, List, Optional, Tuple


def compute_channel_stats(values: List[int]) -> Dict[str, Optional[float]]:
    """Calculates mean (rounded to 1 decimal), min, and max for a single channel."""
    if not values:
        return {"mean": None, "min": None, "max": None}
    mean_val = round(sum(values) / len(values), 1)
    return {
        "mean": mean_val,
        "min": min(values),
        "max": max(values),
    }


def compute_magnitude(sensor2_samples: List[List[int]]) -> Optional[float]:
    """Calculates Mean of sqrt(ch1^2 + ch2^2) over available sensor 2 samples."""
    if not sensor2_samples:
        return None
    mags = [math.hypot(sample[0], sample[1]) for sample in sensor2_samples]
    return round(sum(mags) / len(mags), 1)


class HeartRateDetector:
    """
    Deterministic ECG peak detector and heart rate calculator.
    Satisfies all four peak detection rules:
    1. 800 preceding samples and 1 lookahead sample are available.
    2. Value strictly greater than both immediate neighbors.
    3. Value strictly greater than 0.5 * max of 800 preceding samples.
    4. At least 100 samples after the previous accepted peak.
    """

    def __init__(self):
        self.accepted_peaks: List[int] = []

    def reset(self) -> None:
        self.accepted_peaks.clear()

    def find_peaks_in_range(
        self, s1_ch2_dict: Dict[int, int], start_idx: int, end_idx: int
    ) -> List[int]:
        """
        Detects all peaks with sample indices in [start_idx, end_idx].
        s1_ch2_dict maps sample_index -> channel 2 integer value.
        """
        peaks: List[int] = []
        last_peak: Optional[int] = self.accepted_peaks[-1] if self.accepted_peaks else None

        # Check each candidate index i in [start_idx, end_idx]
        for i in range(start_idx, end_idx + 1):
            # Rule 1: Lookback [i-800, i-1] and lookahead i+1 must be present
            if i + 1 not in s1_ch2_dict or i not in s1_ch2_dict:
                continue

            # Check 800 preceding samples availability
            has_full_history = True
            preceding_vals = []
            for h in range(i - 800, i):
                if h not in s1_ch2_dict:
                    has_full_history = False
                    break
                preceding_vals.append(s1_ch2_dict[h])

            if not has_full_history:
                continue

            val_curr = s1_ch2_dict[i]
            val_prev = s1_ch2_dict[i - 1]
            val_next = s1_ch2_dict[i + 1]

            # Rule 2: Strictly greater than both neighbors
            if val_curr <= val_prev or val_curr <= val_next:
                continue

            # Rule 3: Strictly greater than half of max of preceding 800 samples
            max_preceding = max(preceding_vals)
            if val_curr <= 0.5 * max_preceding:
                continue

            # Rule 4: At least 100 samples after previous accepted peak
            if last_peak is not None and (i - last_peak) < 100:
                continue

            # All 4 rules satisfied
            peaks.append(i)
            last_peak = i

        return peaks


def process_window(
    window_idx: int,
    s1_samples: Dict[int, List[int]],
    s2_samples: Dict[int, List[int]],
    mode: str = "online",
    s1_ch2_history: Optional[Dict[int, int]] = None,
) -> Dict[str, Any]:
    """
    Computes Section 3 window outputs for window w holding samples [400w, 400w+399].
    """
    start_sample = 400 * window_idx
    end_sample = 400 * window_idx + 399

    s1_available_indices = [idx for idx in range(start_sample, end_sample + 1) if idx in s1_samples]
    s2_available_indices = [idx for idx in range(start_sample, end_sample + 1) if idx in s2_samples]

    missing_1 = 400 - len(s1_available_indices)
    missing_2 = 400 - len(s2_available_indices)
    complete = (missing_1 == 0 and missing_2 == 0)

    # Per-channel statistics
    s1_ch1_vals = [s1_samples[idx][0] for idx in s1_available_indices if len(s1_samples[idx]) > 0]
    s1_ch2_vals = [s1_samples[idx][1] for idx in s1_available_indices if len(s1_samples[idx]) > 1]
    s1_ch3_vals = [s1_samples[idx][2] for idx in s1_available_indices if len(s1_samples[idx]) > 2]

    s2_ch1_vals = [s2_samples[idx][0] for idx in s2_available_indices if len(s2_samples[idx]) > 0]
    s2_ch2_vals = [s2_samples[idx][1] for idx in s2_available_indices if len(s2_samples[idx]) > 1]

    s1_ch1_stats = compute_channel_stats(s1_ch1_vals)
    s1_ch2_stats = compute_channel_stats(s1_ch2_vals)
    s1_ch3_stats = compute_channel_stats(s1_ch3_vals)

    s2_ch1_stats = compute_channel_stats(s2_ch1_vals)
    s2_ch2_stats = compute_channel_stats(s2_ch2_vals)

    # Magnitude over available sensor 2 samples
    s2_rows = [s2_samples[idx] for idx in s2_available_indices if len(s2_samples[idx]) >= 2]
    magnitude = compute_magnitude(s2_rows)

    # Heart rate calculation
    heart_rate: Optional[float] = None
    if window_idx >= 4 and s1_ch2_history is not None:
        hr_start = 400 * (window_idx - 4)
        hr_end = 400 * (window_idx + 1) - 1

        # Check if ANY sample in the 5 seconds [hr_start, hr_end] is missing
        missing_in_5s = any(idx not in s1_ch2_history for idx in range(hr_start, hr_end + 1))

        if not missing_in_5s:
            detector = HeartRateDetector()
            # We detect peaks across the entire history up to hr_end
            # starting from 800 (earliest possible peak index)
            peaks = detector.find_peaks_in_range(s1_ch2_history, 800, hr_end)
            # Filter peaks strictly in the last 5-second range [hr_start, hr_end]
            window_peaks = [p for p in peaks if hr_start <= p <= hr_end]

            if len(window_peaks) >= 2:
                gaps = [
                    (window_peaks[k + 1] - window_peaks[k]) / 400.0
                    for k in range(len(window_peaks) - 1)
                ]
                mean_gap_seconds = sum(gaps) / len(gaps)
                if mean_gap_seconds > 0:
                    heart_rate = round(60.0 / mean_gap_seconds, 1)

    result = {
        "window": window_idx,
        "mode": mode,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "channels": {
            "s1_ch1": s1_ch1_stats,
            "s1_ch2": s1_ch2_stats,
            "s1_ch3": s1_ch3_stats,
            "s2_ch1": s2_ch1_stats,
            "s2_ch2": s2_ch2_stats,
        },
        "sensor_1": {
            "ch1": s1_ch1_stats,
            "ch2": s1_ch2_stats,
            "ch3": s1_ch3_stats,
        },
        "sensor_2": {
            "ch1": s2_ch1_stats,
            "ch2": s2_ch2_stats,
        },
        # Flat properties for compatibility with flat-access checkers
        "s1_ch1_mean": s1_ch1_stats["mean"],
        "s1_ch1_min": s1_ch1_stats["min"],
        "s1_ch1_max": s1_ch1_stats["max"],
        "s1_ch2_mean": s1_ch2_stats["mean"],
        "s1_ch2_min": s1_ch2_stats["min"],
        "s1_ch2_max": s1_ch2_stats["max"],
        "s1_ch3_mean": s1_ch3_stats["mean"],
        "s1_ch3_min": s1_ch3_stats["min"],
        "s1_ch3_max": s1_ch3_stats["max"],
        "s2_ch1_mean": s2_ch1_stats["mean"],
        "s2_ch1_min": s2_ch1_stats["min"],
        "s2_ch1_max": s2_ch1_stats["max"],
        "s2_ch2_mean": s2_ch2_stats["mean"],
        "s2_ch2_min": s2_ch2_stats["min"],
        "s2_ch2_max": s2_ch2_stats["max"],
        "magnitude": magnitude,
        "heart_rate": heart_rate,
    }

    return result
