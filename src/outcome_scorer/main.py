"""Outcome scorer — one-shot.

For each (signal × horizon):
  - Skip if outcome already written for that horizon
  - Skip if not enough time has elapsed since signal
  - Fetch price at signal time and at signal_time + horizon
  - Score win/loss/flat
  - Insert signal_outcomes row

After all signals scored, recompute consistency_scores per (strategy × asset × horizon).
"""
import asyncio
import logging
from datetime import timedelta

import sentry_sdk

from outcome_scorer.aggregate import recompute_consistency
from outcome_scorer.db import db
from outcome_scorer.prices import price_at
from outcome_scorer.score import horizon_label, score_outcome
from outcome_scorer.settings import settings

log = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn, traces_sample_rate=0.0)


async def score_all() -> dict:
    totals = {"considered": 0, "scored": 0, "no_price": 0, "errors": 0}
    for h_hours in settings.horizons:
        h_label = horizon_label(h_hours)
        rows = await db.signals_to_score(h_hours, settings.min_signal_age_hours)
        log.info("Horizon %s: %d signals to score", h_label, len(rows))

        for s in rows:
            totals["considered"] += 1
            try:
                signal_ts = s["published_at"]
                eval_ts = signal_ts + timedelta(hours=h_hours)

                p_signal = await price_at(s["asset"], signal_ts)
                p_eval = await price_at(s["asset"], eval_ts)

                if p_signal is None or p_eval is None:
                    # 'expired' = horizon ended without a resolvable outcome (no price feed,
                    # API gated, asset doesn't trade then). Excluded from accuracy stats.
                    await db.insert_outcome(
                        signal_id=s["id"],
                        horizon_label=h_label,
                        outcome="expired",
                        price_at_signal=p_signal,
                        price_at_evaluation=p_eval,
                        price_change_pct=None,
                        notes="no price data",
                    )
                    totals["no_price"] += 1
                    continue

                outcome, pct = score_outcome(
                    s["direction"], p_signal, p_eval, settings.flat_threshold_pct,
                )
                await db.insert_outcome(
                    signal_id=s["id"],
                    horizon_label=h_label,
                    outcome=outcome,
                    price_at_signal=p_signal,
                    price_at_evaluation=p_eval,
                    price_change_pct=pct,
                    notes=None,
                )
                totals["scored"] += 1
            except Exception as exc:  # noqa: BLE001
                log.exception("Score failed for signal %s @ %s: %s", s["id"], h_label, exc)
                totals["errors"] += 1
    return totals


async def aggregate_all() -> int:
    written = 0
    for h_hours in settings.horizons:
        h_label = horizon_label(h_hours)
        n = await recompute_consistency(h_label)
        written += n
        log.info("Aggregated %d consistency rows for horizon %s", n, h_label)
    return written


async def main() -> None:
    _setup_logging()
    log.info("outcome-scorer starting (horizons=%s)", settings.horizons)
    await db.connect()
    try:
        score_totals = await score_all()
        agg_rows = await aggregate_all()
        log.info("Done. score=%s consistency_rows=%d", score_totals, agg_rows)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
