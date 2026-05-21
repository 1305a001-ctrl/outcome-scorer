"""Unit tests for the Quant Rigor validation layer.

Synthetic data with known properties exercises each guarantee:
  * pure-noise returns FAIL the gate (DSR / PBO);
  * a genuinely persistent signal PASSES;
  * purging actually drops label-overlapping train observations;
  * the embargo drops the post-test band;
  * DSR shrinks monotonically as n_trials rises;
  * an all-noise config sweep gives PBO ≈ 0.5; a signal-in-sweep gives PBO well
    below 0.5.

Seeds are fixed so the statistical assertions are deterministic.
"""
import numpy as np
import pandas as pd
import pytest

from outcome_scorer.validation import (
    PBOResult,
    cpcv_splits,
    cscv_pbo,
    deflated_sharpe_ratio,
    expected_max_sharpe,
    probabilistic_sharpe_ratio,
    purged_kfold_splits,
    sharpe_ratio,
    validate,
)
from outcome_scorer.validation.validate import ValidationResult


def _noise(n: int, seed: int) -> np.ndarray:
    return np.random.default_rng(seed).normal(0.0, 1.0, n)


def _signal(n: int, seed: int, mu: float = 0.07, sigma: float = 0.5) -> np.ndarray:
    """I.i.d. positive-drift returns — a genuine (if modest) per-period edge."""
    return np.random.default_rng(seed).normal(mu, sigma, n)


# ─── sharpe_ratio ─────────────────────────────────────────────────────────────


def test_sharpe_zero_variance_is_zero():
    assert sharpe_ratio(np.full(50, 0.3)) == 0.0


def test_sharpe_sign_and_scale():
    r = np.array([1.0, -1.0, 1.0, -1.0, 2.0])
    assert sharpe_ratio(r) == pytest.approx(r.mean() / r.std(ddof=0))


# ─── purged + embargoed K-Fold ──────────────────────────────────────────────


def test_kfold_partitions_test_folds_without_overlap():
    splits = purged_kfold_splits(100, 5)
    test_sets = [set(test.tolist()) for _, test in splits]
    # Test folds are disjoint and cover everything.
    union: set[int] = set()
    for ts in test_sets:
        assert not (union & ts)
        union |= ts
    assert union == set(range(100))


def test_purging_drops_overlapping_labels():
    # Each observation's label lives 5 steps forward, so observations just
    # before a test block overlap it and MUST be purged from training.
    n = 100
    span = 5
    t1 = pd.Series(np.arange(n) + span, index=pd.RangeIndex(n))

    no_purge = purged_kfold_splits(n, 5, t1=None, embargo_pct=0.0)
    purged = purged_kfold_splits(n, 5, t1=t1, embargo_pct=0.0)

    sizes_no = [len(tr) for tr, _ in no_purge]
    sizes_pg = [len(tr) for tr, _ in purged]

    # Purging can only shrink (or equal) the train set, and here it strictly
    # shrinks the interior folds (which have neighbours on both sides).
    assert all(p <= q for p, q in zip(sizes_pg, sizes_no, strict=True))
    assert sum(sizes_pg) < sum(sizes_no)

    # Concretely: no purged train index may have a label overlapping its test.
    for (tr, te), _ in zip(purged, no_purge, strict=True):
        test_t0 = te[0] if len(te) else 0
        test_t1 = int(t1.iloc[te].max())
        for i in tr:
            starts_i = i
            ends_i = int(t1.iloc[i])
            overlaps = (starts_i <= test_t1) and (ends_i >= test_t0)
            assert not overlaps, f"train obs {i} overlaps test [{test_t0},{test_t1}]"


def test_embargo_drops_post_test_band():
    n = 100
    # No purging (point labels) so any train shrinkage is purely the embargo.
    no_embargo = purged_kfold_splits(n, 5, t1=None, embargo_pct=0.0)
    embargoed = purged_kfold_splits(n, 5, t1=None, embargo_pct=0.1)
    assert sum(len(tr) for tr, _ in embargoed) < sum(len(tr) for tr, _ in no_embargo)

    band = int(np.ceil(0.10 * n))
    for tr, te in embargoed:
        last = int(te.max())
        forbidden = set(range(last + 1, min(last + 1 + band, n)))
        assert not (set(tr.tolist()) & forbidden)


def test_kfold_validates_args():
    with pytest.raises(ValueError):
        purged_kfold_splits(100, 1)
    with pytest.raises(ValueError):
        purged_kfold_splits(3, 5)


# ─── CPCV ─────────────────────────────────────────────────────────────────────


def test_cpcv_yields_combinations_count():
    # C(6, 2) = 15 paths.
    assert len(cpcv_splits(120, 6, 2)) == 15
    # C(8, 2) = 28.
    assert len(cpcv_splits(160, 8, 2)) == 28


def test_cpcv_train_and_test_are_disjoint():
    for tr, te in cpcv_splits(120, 6, 2, embargo_pct=0.01):
        assert not (set(tr.tolist()) & set(te.tolist()))


def test_cpcv_validates_args():
    with pytest.raises(ValueError):
        cpcv_splits(120, 1, 1)
    with pytest.raises(ValueError):
        cpcv_splits(120, 6, 6)  # k must be < n_groups


# ─── Deflated Sharpe Ratio ──────────────────────────────────────────────────


def test_psr_increases_with_more_observations():
    sr = 0.1
    short = probabilistic_sharpe_ratio(sr, 50)
    long = probabilistic_sharpe_ratio(sr, 5000)
    assert long > short


def test_expected_max_sharpe_grows_with_trials():
    v = 1.0 / 500
    assert expected_max_sharpe(1, v) == 0.0
    assert expected_max_sharpe(100, v) > expected_max_sharpe(10, v) > 0.0


def test_dsr_shrinks_as_trials_rise():
    sig = _signal(720, seed=7)
    dsrs = [deflated_sharpe_ratio(sig, n_trials=nt)[0] for nt in (1, 5, 20, 100, 1000)]
    # Monotone non-increasing (consecutive pairs) and strictly lower overall.
    assert all(b <= a + 1e-9 for a, b in zip(dsrs[:-1], dsrs[1:], strict=True))
    assert dsrs[-1] < dsrs[0]


def test_dsr_high_for_strong_signal_low_for_noise():
    assert deflated_sharpe_ratio(_signal(720, 1), n_trials=1)[0] > 0.9
    assert deflated_sharpe_ratio(_noise(720, 1), n_trials=1)[0] < 0.6


# ─── PBO via CSCV ─────────────────────────────────────────────────────────────


def test_cscv_undefined_for_single_config():
    res = cscv_pbo(np.random.default_rng(0).normal(0, 1, (200, 1)))
    assert isinstance(res, PBOResult)
    assert np.isnan(res.pbo)
    assert res.n_splits == 0


def test_cscv_requires_even_splits():
    with pytest.raises(ValueError):
        cscv_pbo(np.random.default_rng(0).normal(0, 1, (200, 5)), n_splits=15)


def test_cscv_rejects_non_2d():
    with pytest.raises(ValueError):
        cscv_pbo(np.zeros(10))


def test_pbo_high_for_all_noise_sweep():
    # 30 indistinguishable noise configs → IS-best is luck → PBO ≈ 0.5.
    pbos = []
    for s in range(8):
        cfg = np.random.default_rng(200 + s).normal(0, 1, (960, 30))
        pbos.append(cscv_pbo(cfg, n_splits=10).pbo)
    assert 0.35 < np.mean(pbos) < 0.65


def test_pbo_low_when_one_config_truly_dominates():
    # One config has real drift; the rest are noise → IS-best persists OOS.
    pbos = []
    for s in range(8):
        rng = np.random.default_rng(300 + s)
        cfg = rng.normal(0, 1, (960, 30))
        cfg[:, 0] = rng.normal(0.18, 1.0, 960)
        pbos.append(cscv_pbo(cfg, n_splits=10).pbo)
    assert np.mean(pbos) < 0.3


# ─── validate() end-to-end ────────────────────────────────────────────────────


def test_validate_noise_fails_gate():
    # The headline guarantee: pure noise must NOT pass.
    fails = 0
    for s in range(10):
        res = validate(_noise(720, s), n_trials=20)
        fails += not res.passed
    assert fails == 10


def test_validate_persistent_signal_passes():
    passes = 0
    for s in range(10):
        res = validate(_signal(720, 100 + s), n_trials=1)
        passes += res.passed
    assert passes == 10


def test_validate_returns_full_result_shape():
    res = validate(_signal(600, 3), n_trials=1)
    assert isinstance(res, ValidationResult)
    assert res.n_obs == 600
    assert len(res.purged_cv_scores) == res.meta["n_kfold_splits"] == 5
    assert len(res.cpcv_scores) == res.meta["n_cpcv_paths"] == 15
    assert np.isnan(res.pbo)  # single series → PBO undefined
    assert "PASS" in res.reason


def test_validate_accepts_pandas_series_with_index():
    idx = pd.date_range("2024-01-01", periods=400, freq="h")
    s = pd.Series(_signal(400, 5), index=idx)
    res = validate(s, n_trials=1)
    assert res.meta["has_index"] is True
    assert res.n_obs == 400


def test_validate_pbo_gate_with_config_sweep():
    # All-noise sweep: should fail (PBO ~0.5 and/or weak DSR).
    rng = np.random.default_rng(202)
    noise_cfg = rng.normal(0, 1, (960, 30))
    res_noise = validate(noise_cfg[:, 0], config_returns=noise_cfg)
    assert not res_noise.passed
    assert res_noise.meta["pbo_source"] == "config_returns"

    # One dominant config among noise: low PBO and strong DSR → pass.
    rng = np.random.default_rng(303)
    cfg = rng.normal(0, 1, (960, 30))
    cfg[:, 0] = rng.normal(0.18, 1.0, 960)
    res_sig = validate(cfg[:, 0], config_returns=cfg)
    assert res_sig.pbo < 0.5
    assert res_sig.passed


def test_validate_config_n_trials_at_least_sweep_width():
    rng = np.random.default_rng(11)
    cfg = rng.normal(0, 1, (480, 12))
    res = validate(cfg[:, 0], n_trials=1, config_returns=cfg)
    # Deflation must account for the full breadth of the sweep.
    assert res.n_trials >= 12


def test_validate_dsr_pass_threshold_is_configurable():
    sig = _signal(720, 100)
    lax = validate(sig, n_trials=1, dsr_pass=0.5)
    strict = validate(sig, n_trials=1, dsr_pass=0.999999)
    assert lax.passed
    # A near-1.0 confidence bar is harder to clear than a coin flip.
    assert strict.deflated_sharpe == lax.deflated_sharpe
    assert (strict.deflated_sharpe > 0.999999) == strict.passed


def test_validate_rejects_too_few_obs():
    with pytest.raises(ValueError):
        validate(np.zeros(3), n_splits=5)


def test_validate_rejects_2d_returns():
    with pytest.raises(ValueError):
        validate(np.zeros((10, 2)))


def test_validate_rejects_bad_config_returns():
    with pytest.raises(ValueError):
        validate(_signal(100, 1), config_returns=np.zeros(100))  # not 2-D


def test_cpcv_mean_sharpe_property():
    res = validate(_signal(600, 3), n_trials=1)
    assert res.cpcv_mean_sharpe == pytest.approx(float(np.mean(res.cpcv_scores)))
