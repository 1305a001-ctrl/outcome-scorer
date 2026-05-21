"""Probability of Backtest Overfitting (PBO) via CSCV.

PURE — no I/O. Implements the Combinatorially-Symmetric Cross-Validation
(CSCV) estimator of Bailey, Borwein, López de Prado & Zhu, "The Probability of
Backtest Overfitting" (J. Computational Finance, 2017).

Given a performance matrix ``M`` of shape ``(T, N)`` — ``T`` time observations
by ``N`` candidate configurations — CSCV:

  1. Splits the ``T`` rows into ``S`` disjoint, contiguous submatrices
     (``S`` even).
  2. For every way to choose ``S/2`` submatrices as the in-sample (IS) set
     (the rest are out-of-sample, OOS):
       * rank the configs by IS performance, pick the IS-best ``n*``;
       * find that config's *relative rank* ``w`` in OOS (0..1, 1 == best);
       * record the logit ``λ = ln(w / (1 - w))``.
  3. **PBO = fraction of splits with ``λ <= 0``** — i.e. the IS-best config
     landed in the bottom half OOS. PBO near 0 ⇒ robust; PBO ≥ 0.5 ⇒ the
     selection process is overfit (no better than a coin flip out-of-sample).

When ``N == 1`` PBO is undefined as a *selection*-overfitting measure (there's
nothing to select between); we return ``nan`` and an empty logit array, and the
caller's gate falls back to DSR.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass
from itertools import combinations

import numpy as np


@dataclass(frozen=True)
class PBOResult:
    """Outcome of a CSCV run."""

    pbo: float
    """Probability of backtest overfitting in [0, 1] (nan if N < 2)."""
    logits: np.ndarray
    """Per-split logit λ of the IS-best config's OOS rank."""
    n_splits: int
    """Number of CSCV combinations evaluated."""


def _default_score(block: np.ndarray) -> np.ndarray:
    """Per-config score on a (rows, N) block: mean / std (Sharpe-like).

    Ranking configs by a Sharpe-like statistic (rather than raw mean) is the
    standard CSCV choice and is scale-robust across blocks. NaN-tolerant: a
    config inactive for part of a block (NaN entries) is scored on its finite
    entries; a config with *no* finite entries in a block scores ``-inf`` so it
    can never win in-sample.
    """
    finite = np.isfinite(block)
    all_nan = ~finite.any(axis=0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        mean = np.nanmean(block, axis=0)
        sd = np.nanstd(block, axis=0, ddof=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        score = np.where((sd > 0) & np.isfinite(mean), mean / sd, 0.0)
    return np.where(all_nan, -np.inf, score)


def cscv_pbo(matrix: np.ndarray, n_splits: int = 16) -> PBOResult:
    """Compute PBO via CSCV on a (T, N) performance matrix.

    ``matrix[t, j]`` is the per-period return (or PnL) of configuration ``j``
    at time ``t``. ``n_splits`` (``S``) must be even; it is reduced to the
    largest even number <= T if necessary.
    """
    m = np.asarray(matrix, dtype=float)
    if m.ndim != 2:
        raise ValueError("matrix must be 2-D (T observations x N configs)")
    n_obs, n_cfg = m.shape
    if n_cfg < 2:
        return PBOResult(pbo=float("nan"), logits=np.empty(0), n_splits=0)
    if n_splits % 2 != 0:
        raise ValueError("n_splits (S) must be even")
    if n_splits > n_obs:
        n_splits = n_obs - (n_obs % 2)
    if n_splits < 2:
        return PBOResult(pbo=float("nan"), logits=np.empty(0), n_splits=0)

    # Disjoint contiguous submatrices over the time axis.
    row_blocks = np.array_split(np.arange(n_obs), n_splits)

    logits: list[float] = []
    half = n_splits // 2
    for is_block_ids in combinations(range(n_splits), half):
        is_rows = np.concatenate([row_blocks[b] for b in is_block_ids])
        oos_block_ids = [b for b in range(n_splits) if b not in is_block_ids]
        oos_rows = np.concatenate([row_blocks[b] for b in oos_block_ids])

        is_score = _default_score(m[is_rows])
        oos_score = _default_score(m[oos_rows])

        best_cfg = int(np.argmax(is_score))

        # Relative rank of the IS-best config among OOS scores, in (0, 1).
        # ``rank`` counts configs strictly worse OOS; ties share the midpoint.
        order = np.argsort(oos_score, kind="mergesort")
        ranks = np.empty(n_cfg, dtype=float)
        ranks[order] = np.arange(1, n_cfg + 1, dtype=float)
        w = ranks[best_cfg] / (n_cfg + 1)  # in (0, 1), avoids 0 and 1

        logit = float(np.log(w / (1.0 - w)))
        logits.append(logit)

    logit_arr = np.asarray(logits, dtype=float)
    pbo = float(np.mean(logit_arr <= 0.0)) if logit_arr.size else float("nan")
    return PBOResult(pbo=pbo, logits=logit_arr, n_splits=len(logits))
