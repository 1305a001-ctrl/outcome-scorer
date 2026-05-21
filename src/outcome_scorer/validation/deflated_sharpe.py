"""Deflated Sharpe Ratio (DSR) and supporting statistics.

PURE — no I/O. Implements the DSR of Bailey & López de Prado (AFML ch. 8).

The Sharpe ratio inflates with the number of independent trials a researcher
runs: try enough configurations and one will look great by luck. DSR deflates
the observed (non-annualised) Sharpe ``SR_hat`` by the Sharpe you'd *expect*
from the best of ``n_trials`` random strategies, then reports the probability
that the true Sharpe exceeds that benchmark, accounting for the return
distribution's skew and kurtosis and the sample length.
"""
from __future__ import annotations

import numpy as np
from scipy.stats import norm


def sharpe_ratio(returns: np.ndarray) -> float:
    """Non-annualised Sharpe ratio = mean / std (population std, ddof=0).

    Returns 0.0 for a degenerate (near-zero-variance) series. The tolerance is
    relative to the data scale so a constant series (whose ``std`` is a tiny
    floating-point residual, not exactly 0) is correctly treated as flat.
    """
    r = np.asarray(returns, dtype=float)
    if r.size == 0:
        return 0.0
    sd = r.std(ddof=0)
    scale = max(abs(float(r.mean())), float(np.max(np.abs(r))), 1.0)
    if not np.isfinite(sd) or sd <= 1e-12 * scale:
        return 0.0
    return float(r.mean() / sd)


def _skew_kurt(returns: np.ndarray) -> tuple[float, float]:
    """Sample skewness and *non-excess* kurtosis (normal -> 3.0)."""
    r = np.asarray(returns, dtype=float)
    n = r.size
    sd = r.std(ddof=0)
    scale = max(abs(float(r.mean())), float(np.max(np.abs(r))) if n else 0.0, 1.0)
    if n < 3 or not np.isfinite(sd) or sd <= 1e-12 * scale:
        return 0.0, 3.0
    z = (r - r.mean()) / sd
    skew = float(np.mean(z**3))
    kurt = float(np.mean(z**4))
    return skew, kurt


def expected_max_sharpe(n_trials: int, variance_trials: float) -> float:
    """Expected maximum of ``n_trials`` Sharpe estimates under the null.

    Closed form from Bailey & López de Prado: with ``V`` the cross-trial
    variance of the Sharpe estimates and ``E`` Euler-Mascheroni,

        E[max] ≈ sqrt(V) * ((1 - E) * Z^-1(1 - 1/N) + E * Z^-1(1 - 1/(N e)))

    This is the benchmark Sharpe the *best* of N random trials would hit, i.e.
    the value the observed Sharpe must beat to be credible.

    ``variance_trials`` (``V``) is the variance of the per-observation Sharpe
    estimates *across the trials*. Pass the empirical cross-trial variance when
    you have the trial Sharpes; otherwise use the null sampling variance of a
    single per-observation Sharpe, ``≈ 1 / n_obs`` (see
    :func:`deflated_sharpe_ratio`). Passing ``V = 1`` is almost always wrong:
    that is the variance of an *annualised* Sharpe over one year and grossly
    over-deflates per-observation Sharpes.
    """
    if n_trials <= 1:
        return 0.0
    euler = 0.5772156649015329
    z1 = norm.ppf(1.0 - 1.0 / n_trials)
    z2 = norm.ppf(1.0 - 1.0 / (n_trials * np.e))
    return float(np.sqrt(variance_trials) * ((1.0 - euler) * z1 + euler * z2))


def probabilistic_sharpe_ratio(
    observed_sr: float,
    n_obs: int,
    *,
    benchmark_sr: float = 0.0,
    skew: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """Probabilistic Sharpe Ratio: P(true SR > benchmark_sr).

    ``observed_sr`` / ``benchmark_sr`` are per-observation (non-annualised).
    ``kurtosis`` is non-excess (3.0 == normal). AFML eq. 8.x.
    """
    if n_obs < 2:
        return float("nan")
    denom = np.sqrt(1.0 - skew * observed_sr + (kurtosis - 1.0) / 4.0 * observed_sr**2)
    if denom <= 0 or not np.isfinite(denom):
        return float("nan")
    stat = (observed_sr - benchmark_sr) * np.sqrt(n_obs - 1) / denom
    return float(norm.cdf(stat))


def deflated_sharpe_ratio(
    returns: np.ndarray,
    *,
    n_trials: int = 1,
    variance_trials: float | None = None,
) -> tuple[float, float]:
    """Deflated Sharpe Ratio.

    Returns ``(dsr, observed_sr)`` where ``dsr`` is the probability the true
    Sharpe exceeds the expected-maximum benchmark for ``n_trials`` trials,
    using the sample's own skew/kurtosis and length. ``dsr`` rises toward 1
    for a robust track record and falls toward 0 as ``n_trials`` grows or the
    edge is marginal.

    ``variance_trials`` is the cross-trial variance of the per-observation
    Sharpe estimates. When ``None`` (the usual case — you only have the one
    track record) it defaults to the null sampling variance of a single
    per-observation Sharpe, ``1 / n_obs``. Supply an empirical value when the
    full trial sweep's Sharpes are known.
    """
    r = np.asarray(returns, dtype=float)
    n_obs = r.size
    sr_hat = sharpe_ratio(r)
    skew, kurt = _skew_kurt(r)
    if variance_trials is None:
        variance_trials = 1.0 / n_obs if n_obs > 0 else 1.0
    benchmark = expected_max_sharpe(n_trials, variance_trials)
    dsr = probabilistic_sharpe_ratio(
        sr_hat, n_obs, benchmark_sr=benchmark, skew=skew, kurtosis=kurt
    )
    return dsr, sr_hat
