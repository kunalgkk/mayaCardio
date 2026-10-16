"""
Integration tests for failure injection scenarios.
Directly invokes scenarios from scripts/run_failure_suite.py.
"""

from scripts.run_failure_suite import (
    run_scenario_1_crc_corruption,
    run_scenario_2_duplicate_delivery,
    run_scenario_3_network_outage,
    run_scenario_4_network_restoration,
    run_scenario_5_mock_session_restart,
    run_scenario_6_process_sigkill_recovery,
    run_scenario_7_ground_truth,
)


def test_crc_corrupt_scenario():
    res = run_scenario_1_crc_corruption()
    assert res["result"] == "PASS"


def test_duplicate_delivery_scenario():
    res = run_scenario_2_duplicate_delivery()
    assert res["result"] == "PASS"


def test_network_outage_scenario():
    res = run_scenario_3_network_outage()
    assert res["result"] == "PASS"


def test_network_restoration_scenario():
    res = run_scenario_4_network_restoration()
    assert res["result"] == "PASS"


def test_session_restart_scenario():
    res = run_scenario_5_mock_session_restart()
    assert res["result"] == "PASS"


def test_sigkill_recovery_scenario():
    res = run_scenario_6_process_sigkill_recovery()
    assert res["result"] == "PASS"


def test_ground_truth_parity_scenario():
    res = run_scenario_7_ground_truth()
    assert res["result"] == "PASS"
