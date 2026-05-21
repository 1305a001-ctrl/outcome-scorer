"""OPTIONAL logging of a :class:`ValidationResult` to MLflow / the trial log.

This module is deliberately isolated from the pure core. ``mlflow`` and the
ai-staging trial-log package are imported *lazily inside the functions* so that
importing :mod:`outcome_scorer.validation` (and running ``validate``) never
requires either to be installed. If they're missing, these helpers raise a
clear ``RuntimeError`` rather than failing at import time.

ai-staging specifics (see MEMORY): MLflow tracking URI
``http://100.104.53.104:5000``; trial log under ``/home/benadmin/trials``.
"""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .validate import ValidationResult

DEFAULT_TRACKING_URI = "http://100.104.53.104:5000"
DEFAULT_TRIALS_SRC = "/home/benadmin/trials/src"


def _result_metrics(result: ValidationResult) -> dict[str, float]:
    """Flatten the numeric fields of a result into MLflow-friendly metrics."""
    metrics = {
        "passed": float(result.passed),
        "sharpe": result.sharpe,
        "deflated_sharpe": result.deflated_sharpe,
        "pbo": result.pbo,
        "cpcv_mean_sharpe": result.cpcv_mean_sharpe,
        "n_trials": float(result.n_trials),
        "n_obs": float(result.n_obs),
    }
    if result.purged_cv_scores:
        import numpy as np

        metrics["purged_cv_mean_sharpe"] = float(np.mean(result.purged_cv_scores))
    return metrics


def log_to_mlflow(
    result: ValidationResult,
    *,
    run_name: str | None = None,
    tracking_uri: str = DEFAULT_TRACKING_URI,
    experiment: str = "quant-rigor-validation",
    extra_params: dict[str, Any] | None = None,
) -> str:
    """Log a ValidationResult to MLflow. Returns the MLflow run id.

    Raises ``RuntimeError`` if ``mlflow`` is not installed.
    """
    try:
        import mlflow
    except ImportError as exc:  # pragma: no cover - exercised only without mlflow
        raise RuntimeError(
            "mlflow is not installed; install it to use log_to_mlflow "
            "(the core validate() does not need it)"
        ) from exc

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)
    with mlflow.start_run(run_name=run_name) as run:
        params: dict[str, Any] = {
            "n_trials": result.n_trials,
            "n_obs": result.n_obs,
            "pbo_source": result.meta.get("pbo_source"),
            "embargo_pct": result.meta.get("embargo_pct"),
        }
        if extra_params:
            params.update(extra_params)
        mlflow.log_params(params)
        mlflow.log_metrics(_result_metrics(result))
        mlflow.set_tag("reason", result.reason)
        return run.info.run_id


def log_to_trial_log(
    result: ValidationResult,
    *,
    strategy: str,
    trials_src: str = DEFAULT_TRIALS_SRC,
    extra: dict[str, Any] | None = None,
) -> Any:
    """Record a ValidationResult via the ai-staging trial log.

    Lazily imports ``open_trial`` / ``complete_trial`` from ``trials_src``
    (added to ``sys.path``). Raises ``RuntimeError`` if unavailable. Returns
    whatever ``complete_trial`` returns (typically the trial id/handle).
    """
    if trials_src not in sys.path:
        sys.path.insert(0, trials_src)
    try:
        from trial_log import complete_trial, open_trial  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - infra-only path
        raise RuntimeError(
            f"trial-log package not importable from {trials_src}; "
            "this optional helper only runs on ai-staging"
        ) from exc

    trial = open_trial(
        strategy=strategy,
        kind="validation",
        params={"n_trials": result.n_trials, "n_obs": result.n_obs},
    )
    payload = {
        "passed": result.passed,
        "reason": result.reason,
        "sharpe": result.sharpe,
        "deflated_sharpe": result.deflated_sharpe,
        "pbo": result.pbo,
        "cpcv_mean_sharpe": result.cpcv_mean_sharpe,
    }
    if extra:
        payload.update(extra)
    return complete_trial(trial, metrics=payload)
