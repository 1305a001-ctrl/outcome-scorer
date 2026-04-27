"""Roll outcome rows into consistency_scores per (strategy, asset, horizon)."""
import logging

from outcome_scorer.db import db

log = logging.getLogger(__name__)


async def recompute_consistency(horizon_label: str) -> int:
    """Aggregate signal_outcomes for a horizon into consistency_scores. Returns rows written."""
    rows = await db.consistency_input(horizon_label)
    written = 0
    for r in rows:
        total = int(r["total_signals"])
        correct = int(r["correct_signals"])
        accuracy = correct / total if total > 0 else None

        # Expectancy: average pct change per signal, sign-adjusted by direction.
        # long avg_pct adds as-is; short avg_pct flips sign. weighted by counts.
        n_long = int(r["n_long"] or 0)
        n_short = int(r["n_short"] or 0)
        avg_long = float(r["avg_pct_long"] or 0)
        avg_short = float(r["avg_pct_short"] or 0)
        if total > 0:
            expectancy = (n_long * avg_long + n_short * (-avg_short)) / total
        else:
            expectancy = None

        await db.upsert_consistency(
            strategy_id=r["strategy_id"],
            asset=r["asset"],
            period_start=r["period_start"],
            period_end=r["period_end"],
            horizon_label=horizon_label,
            total_signals=total,
            correct_signals=correct,
            accuracy=accuracy,
            expectancy=expectancy,
        )
        written += 1
        log.info("consistency: strategy=%s asset=%s horizon=%s n=%d acc=%.2f exp=%+.4f",
                 r["strategy_id"], r["asset"], horizon_label, total,
                 accuracy or 0, expectancy or 0)
    return written
