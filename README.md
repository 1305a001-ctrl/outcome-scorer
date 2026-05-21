# outcome-scorer

Closes the feedback loop. For every signal in `market_signals`, fetches the asset price at `published_at` and at `published_at + horizon`, scores `win / loss / flat`, writes `signal_outcomes`. Then aggregates into `consistency_scores` so you can finally answer **"which strategy actually has edge?"**.

For system context, read [`infra-core/docs/ARCHITECTURE.md`](https://github.com/1305a001-ctrl/infra-core/blob/main/docs/ARCHITECTURE.md) first.

## What it does

1. Reads signals older than each `HORIZONS_HOURS` (4h / 24h / 7d by default).
2. Skips any that already have a `signal_outcomes` row at that horizon (idempotent — safe to re-run).
3. Fetches historical 1h close prices:
   - Crypto via Binance public klines API (no key)
   - US equities via Finnhub `/stock/candle` — **requires Finnhub Premium**; the free tier returns HTTP 403 on this endpoint. Until a paid key (or Alpaca historical bars adapter) is wired, US equity outcomes are recorded as `expired`.
   - KLSE / unsupported → recorded as `expired` with note `no price data`

`expired` outcomes are excluded from `consistency_scores.accuracy` so the metric represents only *evaluable* signals.
4. Scores: `long → win if pct > +threshold`, `short → win if pct < -threshold`, else flat. (`FLAT_THRESHOLD_PCT` default 0.5%.)
5. Recomputes `consistency_scores` per (strategy × asset × horizon).

## Module map

```
src/outcome_scorer/
├── main.py        # one-shot entry; orchestrates score_all + aggregate_all
├── settings.py    # horizons, threshold, API URLs
├── db.py          # asyncpg pool + JSONB codec; reads signals_to_score, writes outcomes
├── prices.py      # Binance klines + Finnhub candle adapters
├── score.py       # PURE — score_outcome, horizon_label
└── aggregate.py   # rolls signal_outcomes into consistency_scores
```

`score.py` is pure-function and fully covered by tests.

## Quant Rigor validation layer (`outcome_scorer.validation`)

Answering "which strategy has edge?" by accuracy alone promotes overfit
patterns to live — exactly how `chainlink_lag` looked great in-sample and then
dead-ended. This subpackage adds an out-of-sample rigor gate that decides
whether a backtested edge is real or an artifact, implementing four techniques
from López de Prado's *Advances in Financial Machine Learning* (AFML) and
Bailey & López de Prado **directly on numpy/pandas/scipy** — it deliberately
does **not** depend on `mlfinlab` (abandoned; fails on numpy 2.x).

```
src/outcome_scorer/validation/
├── cross_validation.py  # Purged+Embargoed K-Fold (AFML 7) & CPCV (AFML 12)
├── deflated_sharpe.py   # PSR + Deflated Sharpe Ratio (AFML 8)
├── pbo.py               # Probability of Backtest Overfitting via CSCV
├── validate.py          # PURE entry point: validate() -> ValidationResult
└── mlflow_log.py        # OPTIONAL, lazy-imported MLflow / trial-log sink
```

The core `validate()` is **pure** (no I/O) and unit-tested offline on synthetic
data with known properties (noise fails, persistent signal passes, purging
drops overlapping labels, DSR shrinks as trials rise).

```python
import numpy as np
from outcome_scorer.validation import validate

returns = np.asarray(strategy_pnl)          # 1-D per-period returns / Series
res = validate(returns, n_trials=20)        # n_trials = configs you tried
res.passed            # bool — gate: PBO < 0.5 AND DSR > dsr_pass (default .5)
res.deflated_sharpe   # P(true Sharpe > expected-max benchmark for n_trials)
res.pbo               # nan unless a config sweep is supplied (see below)
res.purged_cv_scores  # per-fold OOS Sharpe (purged K-Fold)
res.cpcv_scores       # per-path OOS Sharpe distribution (CPCV)

# Proper PBO needs the candidate set: pass the trial sweep (T x N matrix).
res = validate(best_cfg_returns, config_returns=sweep_matrix, t1=label_t1)
```

PBO measures *selection* overfitting, so it's only defined across competing
configurations: pass `config_returns` (the T×N sweep) to compute it, otherwise
the gate falls back to the Deflated Sharpe (which already penalises `n_trials`).
Use `t1` (a label-lifespan series) to drive purging when labels span multiple
bars. Optional, non-pure logging lives in `mlflow_log` (lazy imports so the
core never hard-depends on MLflow or the ai-staging trial log).

## Wire-up

No new tables — uses the existing `signal_outcomes` and `consistency_scores`.

```bash
# Manual run (idempotent — safe to invoke whenever)
docker compose -f infra-core/compose/outcome-scorer/docker-compose.yml run --rm outcome-scorer
```

For ongoing scoring, schedule it via systemd timer (mirror of `news-pipeline.timer`) — once daily is plenty since the fastest horizon is 4h.

## Tests

```bash
pip install -e '.[dev]'
pytest -q
```
