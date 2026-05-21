"""Time-series cross-validation with purging and embargo.

PURE — no I/O. Implements two splitters from López de Prado, *Advances in
Financial Machine Learning* (AFML):

  * ``purged_kfold_splits``  — AFML ch. 7: K-Fold where any train observation
    whose label lifespan (``[t0, t1]``) overlaps the test window is *purged*,
    plus a fractional *embargo* applied after each test block.
  * ``cpcv_splits``          — AFML ch. 12: Combinatorial Purged CV. Splits the
    series into ``n_groups`` contiguous groups, then for every combination of
    ``n_test_groups`` test groups yields a (train_idx, test_groups) split with
    the same purge + embargo applied. This produces ``C(n_groups, k)`` splits
    and a richer distribution of out-of-sample paths than plain K-Fold.

Both work on a label-lifespan series ``t1`` indexed like the data, where
``t1[i]`` is the time at which observation ``i``'s label is realised. When
``t1`` is None we treat each observation as a point label (``t1 == index``),
which degenerates purging to "drop the boundary-adjacent observations only".
"""
from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd


def _as_t1(t1: pd.Series | None, n: int) -> pd.Series:
    """Return a label-lifespan series of length ``n``.

    If ``t1`` is None, build a point-label series with an integer RangeIndex
    where each label resolves at its own index (no forward overlap).
    """
    if t1 is None:
        idx = pd.RangeIndex(n)
        return pd.Series(idx, index=idx)
    if len(t1) != n:
        raise ValueError(f"t1 has length {len(t1)} but data has length {n}")
    return t1


def _purge_train(
    train_idx: np.ndarray,
    test_t0: object,
    test_t1: object,
    t1: pd.Series,
) -> np.ndarray:
    """Drop train observations whose label lifespan overlaps the test window.

    An observation ``i`` (label living over ``[index_i, t1_i]``) overlaps the
    test window ``[test_t0, test_t1]`` when ``index_i <= test_t1`` *and*
    ``t1_i >= test_t0``. Those are removed from the training set (AFML 7.4.1).
    """
    starts = t1.index.to_numpy()
    ends = t1.to_numpy()
    overlaps = (starts <= test_t1) & (ends >= test_t0)
    overlap_positions = np.flatnonzero(overlaps)
    return np.setdiff1d(train_idx, overlap_positions, assume_unique=False)


def _embargo_positions(
    test_positions: np.ndarray,
    n: int,
    embargo_pct: float,
) -> np.ndarray:
    """Positions to embargo immediately *after* a test block.

    Embargo removes a band of ``ceil(embargo_pct * n)`` observations following
    the end of the test set so that serially-correlated leakage just after the
    test window cannot contaminate training (AFML 7.4.2).
    """
    if embargo_pct <= 0 or test_positions.size == 0:
        return np.empty(0, dtype=int)
    band = int(np.ceil(embargo_pct * n))
    if band == 0:
        return np.empty(0, dtype=int)
    last = int(test_positions.max())
    start = last + 1
    stop = min(last + 1 + band, n)
    return np.arange(start, stop, dtype=int)


def purged_kfold_splits(
    n: int,
    n_splits: int = 5,
    *,
    t1: pd.Series | None = None,
    embargo_pct: float = 0.0,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Yield purged + embargoed K-Fold (train_idx, test_idx) position arrays.

    Test folds are contiguous blocks (time order preserved). For each fold the
    training set is everything *not* in the test block, minus observations
    purged for label overlap, minus the post-test embargo band.

    Indices are positional (0..n-1), so callers can slice numpy arrays or use
    ``.iloc`` on pandas objects regardless of their index.
    """
    if n_splits < 2:
        raise ValueError("n_splits must be >= 2")
    if n_splits > n:
        raise ValueError(f"n_splits={n_splits} cannot exceed n={n}")
    t1 = _as_t1(t1, n)

    all_pos = np.arange(n)
    fold_bounds = np.array_split(all_pos, n_splits)

    splits: list[tuple[np.ndarray, np.ndarray]] = []
    for test_positions in fold_bounds:
        if test_positions.size == 0:
            continue
        test_t0 = t1.index[test_positions[0]]
        test_t1 = t1.iloc[test_positions].max()

        train_idx = np.setdiff1d(all_pos, test_positions, assume_unique=True)
        train_idx = _purge_train(train_idx, test_t0, test_t1, t1)

        embargoed = _embargo_positions(test_positions, n, embargo_pct)
        if embargoed.size:
            train_idx = np.setdiff1d(train_idx, embargoed, assume_unique=False)

        splits.append((train_idx, np.asarray(test_positions)))
    return splits


def cpcv_splits(
    n: int,
    n_groups: int = 6,
    n_test_groups: int = 2,
    *,
    t1: pd.Series | None = None,
    embargo_pct: float = 0.0,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Combinatorial Purged CV splits (AFML ch. 12).

    Partition the ``n`` observations into ``n_groups`` contiguous groups, then
    for each of the ``C(n_groups, n_test_groups)`` ways to choose the test
    groups, build a split whose test set is the union of the chosen groups and
    whose train set is the remainder, purged for label overlap against *every*
    chosen test group and embargoed after each.

    Returns a list of (train_idx, test_idx) positional arrays — one per
    combination.
    """
    if n_groups < 2:
        raise ValueError("n_groups must be >= 2")
    if not 1 <= n_test_groups < n_groups:
        raise ValueError("n_test_groups must satisfy 1 <= k < n_groups")
    if n_groups > n:
        raise ValueError(f"n_groups={n_groups} cannot exceed n={n}")
    t1 = _as_t1(t1, n)

    all_pos = np.arange(n)
    groups = np.array_split(all_pos, n_groups)

    splits: list[tuple[np.ndarray, np.ndarray]] = []
    for combo in combinations(range(n_groups), n_test_groups):
        test_positions = np.concatenate([groups[g] for g in combo])
        test_positions.sort()

        train_idx = np.setdiff1d(all_pos, test_positions, assume_unique=True)

        # Purge + embargo against each contiguous chosen group separately so a
        # discontiguous test set (e.g. groups {0, 3}) purges around both.
        for g in combo:
            block = groups[g]
            if block.size == 0:
                continue
            test_t0 = t1.index[block[0]]
            test_t1 = t1.iloc[block].max()
            train_idx = _purge_train(train_idx, test_t0, test_t1, t1)
            embargoed = _embargo_positions(block, n, embargo_pct)
            if embargoed.size:
                train_idx = np.setdiff1d(train_idx, embargoed, assume_unique=False)

        splits.append((train_idx, test_positions))
    return splits
