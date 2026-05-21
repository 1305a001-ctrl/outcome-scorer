"""Quant Rigor Layer — out-of-sample validation for strategy returns.

A small, dependency-light implementation (numpy / pandas / scipy) of four
techniques from López de Prado's *Advances in Financial Machine Learning*
and Bailey & López de Prado, used to decide whether a backtested edge is real
or an artifact of overfitting:

  * Purged + Embargoed K-Fold CV  (``cross_validation``)
  * Combinatorial Purged CV       (``cross_validation``)
  * Deflated Sharpe Ratio         (``deflated_sharpe``)
  * Probability of Backtest Overfitting via CSCV (``pbo``)

Public entry point: :func:`validate`, returning a :class:`ValidationResult`.
The core is PURE (no I/O); :mod:`outcome_scorer.validation.mlflow_log` adds an
optional thin logger to MLflow / the ai-staging trial log.

This intentionally does NOT depend on ``mlfinlab`` (abandoned, fails on numpy
2.x); the four methods are implemented directly against the references.
"""
from .cross_validation import cpcv_splits, purged_kfold_splits
from .deflated_sharpe import (
    deflated_sharpe_ratio,
    expected_max_sharpe,
    probabilistic_sharpe_ratio,
    sharpe_ratio,
)
from .pbo import PBOResult, cscv_pbo
from .validate import ValidationResult, validate

__all__ = [
    "validate",
    "ValidationResult",
    "purged_kfold_splits",
    "cpcv_splits",
    "deflated_sharpe_ratio",
    "probabilistic_sharpe_ratio",
    "expected_max_sharpe",
    "sharpe_ratio",
    "cscv_pbo",
    "PBOResult",
]
