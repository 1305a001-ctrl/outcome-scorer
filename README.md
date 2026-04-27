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
