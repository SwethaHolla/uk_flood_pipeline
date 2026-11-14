"""Chaos-classification tests (Phase B item 1).

The audit found the chaos harness recorded a connection refusal as
"[OK] Handled" because the broad except-set marked ``ok=True``. These tests pin
the corrected verdict logic.
"""

from __future__ import annotations

from scripts.chaos import _classify


def test_connection_refusal_is_unhandled_for_http_endpoints():
    _, verdict = _classify(2, "error", "ConnectError: refused")
    assert verdict == "[FAIL] Unhandled"


def test_connection_refusal_is_handled_for_transport_faults():
    for scenario_id in (1, 6, 7):
        _, verdict = _classify(scenario_id, "error", "ReadTimeout: slow")
        assert verdict == "[OK] Handled", f"scenario {scenario_id} should tolerate transport error"


def test_expected_status_codes_are_handled():
    assert _classify(2, 429, None)[1] == "[OK] Handled"
    assert _classify(3, 500, None)[1] == "[OK] Handled"


def test_wrong_status_code_is_unhandled():
    assert _classify(2, 200, None)[1] == "[FAIL] Unhandled"
    assert _classify(3, 404, None)[1] == "[FAIL] Unhandled"


def test_bad_body_endpoints_expect_200():
    assert _classify(4, 200, None)[1] == "[OK] Handled"
    assert _classify(5, 200, None)[1] == "[OK] Handled"
    assert _classify(4, 500, None)[1] == "[FAIL] Unhandled"
