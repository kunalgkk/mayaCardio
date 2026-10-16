"""
Unit tests for canonical window signal processing and ECG peak detection.
Verifies Section 3 and Criterion 7:
- Window boundaries [400w, 400w + 399].
- Completeness and missing counts.
- Channel statistics (mean, min, max, null).
- Magnitude calculation.
- All 4 strict peak detection rules.
- 5-second heart rate calculation and null conditions.
- 1-decimal-place rounding.
"""

import math
import pytest
from mayacardio.processor import (
    compute_channel_stats,
    compute_magnitude,
    HeartRateDetector,
    process_window,
)


def test_channel_stats():
    # Regular values
    stats = compute_channel_stats([10, 20, 25])
    assert stats["mean"] == 18.3
    assert stats["min"] == 10
    assert stats["max"] == 25

    # Empty channel
    empty_stats = compute_channel_stats([])
    assert empty_stats["mean"] is None
    assert empty_stats["min"] is None
    assert empty_stats["max"] is None


def test_magnitude():
    # ch1=3, ch2=4 -> sqrt(3^2 + 4^2) = 5.0
    s2_data = [[3, 4], [3, 4], [3, 4]]
    mag = compute_magnitude(s2_data)
    assert mag == 5.0

    # Empty
    assert compute_magnitude([]) is None


def test_window_boundaries_and_completeness():
    w = 2
    start_sample = 400 * w  # 800
    end_sample = 400 * w + 399  # 1199

    # Provide all 400 samples for sensor 1 and sensor 2
    s1_samples = {idx: [100, 200, 300] for idx in range(start_sample, end_sample + 1)}
    s2_samples = {idx: [400000, 150000] for idx in range(start_sample, end_sample + 1)}

    res = process_window(w, s1_samples, s2_samples, mode="offline")
    assert res["window"] == 2
    assert res["mode"] == "offline"
    assert res["missing_1"] == 0
    assert res["missing_2"] == 0
    assert res["complete"] is True
    assert res["magnitude"] is not None

    # Partial missing
    del s1_samples[850]
    res_partial = process_window(w, s1_samples, s2_samples, mode="offline")
    assert res_partial["missing_1"] == 1
    assert res_partial["complete"] is False


def test_peak_detection_rules():
    detector = HeartRateDetector()

    # Rule 1 test: if fewer than 800 preceding samples exist, not a peak
    data_dict = {}
    # Build flat baseline of 1000 from sample 0 to 799
    for i in range(800):
        data_dict[i] = 1000

    # Sample 800 has a spike, but history is only 800 [0 to 799] and lookahead is 801
    data_dict[800] = 5000  # candidate peak
    data_dict[801] = 1000  # neighbor

    # Sample 800 has preceding 800 samples [0..799] and lookahead 801
    peaks = detector.find_peaks_in_range(data_dict, 0, 800)
    assert 800 in peaks

    # Rule 2: must be strictly greater than both neighbors
    data_flat = {i: 1000 for i in range(802)}
    peaks_flat = detector.find_peaks_in_range(data_flat, 0, 800)
    assert len(peaks_flat) == 0

    # Rule 3: must be strictly greater than 0.5 * max of 800 preceding samples
    # Max of preceding is 1000. If candidate is 400 (<= 500), it should be rejected.
    data_dict[800] = 400
    detector.reset()
    assert 800 not in detector.find_peaks_in_range(data_dict, 800, 800)

    # Rule 4: must be at least 100 samples after previous peak
    data_dict[800] = 5000
    data_dict[850] = 5000  # 50 samples later (less than 100)
    data_dict[849] = 1000
    data_dict[851] = 1000
    detector.reset()
    peaks_sep = detector.find_peaks_in_range(data_dict, 800, 850)
    assert 800 in peaks_sep
    assert 850 not in peaks_sep  # rejected due to < 100 samples separation


def test_heart_rate_window_calculation():
    # Window w < 4 should yield heart_rate = None
    res_w2 = process_window(2, {}, {}, mode="online")
    assert res_w2["heart_rate"] is None

    # Window w = 4 with synthetic 60 bpm signal (peak every 400 samples)
    # Range is [400*(4-4), 400*5 - 1] = [0, 1999]
    # In 2000 samples at 60 bpm, peaks occur at e.g. 800, 1200, 1600
    history = {}
    for i in range(2001):
        history[i] = 1000

    # Add peaks at 800, 1200, 1600
    for p in (800, 1200, 1600):
        history[p] = 5000

    s1_samples = {idx: [100, history[idx], 200] for idx in range(1600, 2000)}
    s2_samples = {idx: [400000, 150000] for idx in range(1600, 2000)}

    res_w4 = process_window(4, s1_samples, s2_samples, mode="online", s1_ch2_history=history)
    # Mean gap: (1600 - 800) / 2 = 400 samples = 1.0 second
    # HR = 60 / 1.0 = 60.0 bpm
    assert res_w4["heart_rate"] == 60.0
