"""Public entry point for the Quant Rigor validation layer.

PURE — no I/O. ``validate()`` takes a strategy's per-period returns (or a
labeled signal series) and runs, in order:

  1. Purged + Embargoed K-Fold CV  (AFML ch. 7)
  2. Combinatorial Purged CV (CPCV) (AFML ch. 12)
  3. Deflated Sharpe Ratio (DSR)    (AFML ch. 8 / Bailey & López de Prado)
  4. Probability of Backtest Overfitting (PBO) via CSCV
     (Bailey, Borwein, López de Prado & Zhu)

Overall **pass** requires the Deflated Sharpe to clear ``dsr_pass`` AND the
PBO to be below ``pbo_pass``. PBO measures *selection* overfitting and is only
defined across competing configurations, so when a single series is passed
(nothing to select between) PBO is reported as ``nan`` and the gate falls back
to DSR alone — DSR already penalises the number of trials directly.

Note on thresholds: the spec phrases the gate as "PBO < 0.5 AND DSR > 0". DSR
is a *probability* in [0, 1] (P[true Sharpe > deflated benchmark]); a literal
``> 0`` is trivially satisfied, so the meaningful default here is
``dsr_pass = 0.5`` ("more likely than not the edge survives deflation"),
configurable via the ``dsr_pass`` argument (set 0.95 for a 95%-confidence
gate). ``pbo_pass`` defaults to 0.5 per the reference.

Keep this module free of any database / network / MLflow imports so the whole
thing is unit-testable offline. Logging a result to MLflow / the trial log
lives in the optional ``mlflow_log`` module.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from .cross_validation import cpcv_splits, purged_kfold_splits
from .deflated_sharpe import deflated_sharpe_ratio, sharpe_ratio
from .pbo import PBOResult, cscv_pbo

# Defaults. DSR is a probability; "edge present" means it clears a coin flip.
_DSR_PASS_DEFAULT = 0.5
_PBO_PASS_DEFAULT = 0.5


@dataclass(frozen=True)
class ValidationResult:
    """Structured output of :func:`validate`.

    All Sharpe figures are per-observation (non-annualised), matching the
    returns granularity passed in.
    """

    passed: bool
    """Overall gate: PBO < 0.5 AND DSR > 0 (PBO ignored if undefined)."""
    reason: str
    """Human-readable explanation of the pass/fail decision."""

    sharpe: float
    """Observed Sharpe of the full return series."""
    deflated_sharpe: float
    """DSR — P(true Sharpe > expected-max benchmark for n_trials)."""
    pbo: float
    """Probability of backtest overfitting (nan when undefined)."""

    purged_cv_scores: list[float]
    """Per-fold OOS Sharpe from purged + embargoed K-Fold CV."""
    cpcv_scores: list[float]
    """Per-path OOS Sharpe from Combinatorial Purged CV (the distribution)."""

    n_trials: int
    """Number of trials assumed when deflating the Sharpe."""
    n_obs: int
    """Length of the return series."""
    meta: dict = field(default_factory=dict)
    """Extra diagnostics (means, embargo, split counts, thresholds)."""

    @property
    def cpcv_mean_sharpe(self) -> float:
        """Mean of the CPCV OOS Sharpe distribution (nan if empty)."""
        return float(np.mean(self.cpcv_scores)) if self.cpcv_scores else float("nan")


def _coerce_returns(returns: pd.Series | np.ndarray) -> tuple[np.ndarray, pd.Series | None]:
    """Return ``(values, index_series_or_None)`` from a Series/ndarray."""
    if isinstance(returns, pd.Series):
        return returns.to_numpy(dtype=float), returns
    arr = np.asarray(returns, dtype=float)
    if arr.ndim != 1:
        raise ValueError("returns must be 1-D")
    return arr, None


def _oos_sharpe_per_split(
    values: np.ndarray,
    splits: list[tuple[np.ndarray, np.ndarray]],
) -> list[float]:
    """Sharpe on each split's *test* (OOS) slice."""
    out: list[float] = []
    for _train_idx, test_idx in splits:
        if test_idx.size == 0:
            continue
        out.append(sharpe_ratio(values[test_idx]))
    return out


def validate(
    returns: pd.Series | np.ndarray,
    *,
    t1: pd.Series | None = None,
    n_trials: int = 1,
    config_returns: pd.DataFrame | np.ndarray | None = None,
    n_splits: int = 5,
    cpcv_groups: int = 6,
    cpcv_test_groups: int = 2,
    embargo_pct: float = 0.01,
    cscv_splits: int = 16,
    dsr_pass: float = _DSR_PASS_DEFAULT,
    pbo_pass: float = _PBO_PASS_DEFAULT,
) -> ValidationResult:
    """Run the four-stage Quant Rigor validation on a return series.

    Parameters
    ----------
    returns:
        Per-period strategy returns (or labeled signal P&L). 1-D Series/array.
    t1:
        Optional label-lifespan series (``t1[i]`` = time observation ``i``'s
        label resolves), indexed like ``returns``. Drives purging. If None,
        each observation is treated as a point label.
    n_trials:
        Number of strategy configurations/trials tried during research. Feeds
        the Deflated Sharpe Ratio — higher ``n_trials`` ⇒ harsher deflation.
    config_returns:
        Optional (T, N) matrix of the candidate configs' per-period returns
        (the research trial sweep). When given, PBO is computed across these
        real candidates — the proper selection-overfitting test. When None,
        PBO is left undefined (nan) and the gate falls back to DSR.
    n_splits:
        Folds for purged K-Fold CV.
    cpcv_groups, cpcv_test_groups:
        ``N`` groups and ``k`` test-groups for CPCV — yields C(N, k) paths.
    embargo_pct:
        Fractional embargo applied after each test block in both CV schemes.
    cscv_splits:
        Even number of submatrices ``S`` for the CSCV/PBO estimator.
    dsr_pass:
        Minimum Deflated Sharpe (a probability) to pass. Default 0.5; set 0.95
        for a 95%-confidence gate.
    pbo_pass:
        Maximum PBO to pass. Default 0.5 per Bailey et al.

    Returns
    -------
    ValidationResult
    """
    values, idx_series = _coerce_returns(returns)
    n_obs = values.size
    if n_obs < n_splits:
        raise ValueError(f"need at least n_splits={n_splits} observations, got {n_obs}")

    # 1) Purged + embargoed K-Fold CV.
    kfold = purged_kfold_splits(n_obs, n_splits, t1=t1, embargo_pct=embargo_pct)
    purged_cv_scores = _oos_sharpe_per_split(values, kfold)

    # 2) Combinatorial Purged CV.
    cpcv = cpcv_splits(
        n_obs, cpcv_groups, cpcv_test_groups, t1=t1, embargo_pct=embargo_pct
    )
    cpcv_scores = _oos_sharpe_per_split(values, cpcv)

    # Parse the optional config sweep up front — it informs both DSR (number
    # of trials + empirical cross-trial Sharpe variance) and PBO.
    cfg: np.ndarray | None = None
    if config_returns is not None:
        cfg = (
            config_returns.to_numpy(dtype=float)
            if isinstance(config_returns, pd.DataFrame)
            else np.asarray(config_returns, dtype=float)
        )
        if cfg.ndim != 2:
            raise ValueError("config_returns must be 2-D (T observations x N configs)")

    # 3) Deflated Sharpe Ratio. When the trial sweep is known, deflate using
    #    its actual breadth (n_configs) and the empirical cross-trial variance
    #    of the per-observation Sharpes; otherwise use the caller's n_trials
    #    and the null sampling variance (1/n_obs, handled downstream).
    dsr_n_trials = n_trials
    variance_trials: float | None = None
    if cfg is not None and cfg.shape[1] >= 2:
        cfg_sharpes = np.array([sharpe_ratio(cfg[:, j]) for j in range(cfg.shape[1])])
        dsr_n_trials = max(n_trials, cfg.shape[1])
        variance_trials = float(np.var(cfg_sharpes, ddof=1))
    dsr, sr_hat = deflated_sharpe_ratio(
        values, n_trials=dsr_n_trials, variance_trials=variance_trials
    )

    # 4) PBO via CSCV. PBO measures *selection* overfitting and is only
    #    well-defined across a set of competing configurations. When the caller
    #    supplies ``config_returns`` (the trial sweep), CSCV runs across those
    #    real candidates. With a single series there is nothing to select
    #    between, so PBO is left undefined (nan) and the gate falls back to the
    #    Deflated Sharpe Ratio, which already penalises ``n_trials`` directly.
    if cfg is not None:
        pbo_res: PBOResult = cscv_pbo(cfg, n_splits=cscv_splits)
        pbo_source = "config_returns"
    else:
        pbo_res = PBOResult(pbo=float("nan"), logits=np.empty(0), n_splits=0)
        pbo_source = "undefined_single_config"

    # Gate: PBO < pbo_pass AND DSR > dsr_pass. PBO undefined ⇒ fall back to DSR.
    dsr_ok = bool(np.isfinite(dsr) and dsr > dsr_pass)
    pbo_defined = bool(np.isfinite(pbo_res.pbo))
    pbo_ok = (not pbo_defined) or pbo_res.pbo < pbo_pass
    passed = dsr_ok and pbo_ok

    if not dsr_ok:
        reason = f"FAIL: DSR={dsr:.3f} <= {dsr_pass} (insufficient deflated edge)"
    elif pbo_defined and not pbo_ok:
        reason = f"FAIL: PBO={pbo_res.pbo:.3f} >= {pbo_pass} (backtest overfit)"
    elif not pbo_defined:
        reason = f"PASS (DSR-only): DSR={dsr:.3f} > {dsr_pass}; PBO undefined (single config)"
    else:
        reason = f"PASS: PBO={pbo_res.pbo:.3f} < {pbo_pass} and DSR={dsr:.3f} > {dsr_pass}"

    meta = {
        "embargo_pct": embargo_pct,
        "n_splits": n_splits,
        "cpcv_groups": cpcv_groups,
        "cpcv_test_groups": cpcv_test_groups,
        "n_kfold_splits": len(kfold),
        "n_cpcv_paths": len(cpcv),
        "pbo_n_splits": pbo_res.n_splits,
        "pbo_source": pbo_source,
        "pbo_logit_mean": float(np.mean(pbo_res.logits)) if pbo_res.logits.size else float("nan"),
        "dsr_threshold": dsr_pass,
        "pbo_threshold": pbo_pass,
        "dsr_variance_trials": variance_trials,
        "has_index": idx_series is not None,
    }

    return ValidationResult(
        passed=passed,
        reason=reason,
        sharpe=sr_hat,
        deflated_sharpe=dsr,
        pbo=pbo_res.pbo,
        purged_cv_scores=purged_cv_scores,
        cpcv_scores=cpcv_scores,
        n_trials=dsr_n_trials,
        n_obs=n_obs,
        meta=meta,
    )
