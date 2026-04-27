import pytest

from outcome_scorer.score import horizon_label, score_outcome

# ─── score_outcome ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("p1,p2,expected_outcome,sign", [
    (100, 102, "win", +0.02),    # long, +2% → win
    (100, 99.5, "flat", -0.005),  # long, -0.5% (below threshold) → flat
    (100, 95, "loss", -0.05),    # long, -5% → loss
    (100, 100, "flat", 0.0),
])
def test_long_outcomes(p1, p2, expected_outcome, sign):
    out, pct = score_outcome("long", p1, p2, flat_threshold_pct=0.005)
    assert out == expected_outcome
    assert pct == pytest.approx(sign, rel=1e-3)


@pytest.mark.parametrize("p1,p2,expected", [
    (100, 95, "win"),     # short, -5% → win
    (100, 102, "loss"),   # short, +2% → loss
    (100, 100.4, "flat"),  # short, +0.4% (below threshold) → flat
])
def test_short_outcomes(p1, p2, expected):
    out, _ = score_outcome("short", p1, p2)
    assert out == expected


def test_neutral_and_watch_always_flat():
    assert score_outcome("neutral", 100, 200)[0] == "flat"
    assert score_outcome("watch", 100, 50)[0] == "flat"


def test_zero_signal_price_doesnt_div_by_zero():
    assert score_outcome("long", 0, 100) == ("flat", 0.0)


def test_threshold_boundary():
    # At exactly +threshold → flat (strict > comparison)
    out, _ = score_outcome("long", 100, 100.5, flat_threshold_pct=0.005)
    assert out == "flat"
    # Just above → win
    out, _ = score_outcome("long", 100, 100.51, flat_threshold_pct=0.005)
    assert out == "win"


# ─── horizon_label ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("hours,expected", [
    (1, "1h"),
    (4, "4h"),
    (12, "12h"),
    (24, "1d"),
    (48, "2d"),
    (168, "7d"),
])
def test_horizon_label(hours, expected):
    assert horizon_label(hours) == expected
