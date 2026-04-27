"""Pure scoring logic. No I/O — testable in isolation."""
from typing import Literal

Outcome = Literal["win", "loss", "flat"]


def score_outcome(
    direction: str,
    price_at_signal: float,
    price_at_eval: float,
    flat_threshold_pct: float = 0.005,
) -> tuple[Outcome, float]:
    """Score a directional signal against actual price movement.

    Returns (outcome, price_change_pct).

    Rules:
      long  → win if pct > +threshold; loss if pct < -threshold; else flat
      short → win if pct < -threshold; loss if pct > +threshold; else flat
      neutral / watch → always flat (no directional claim)
    """
    if price_at_signal == 0:
        return "flat", 0.0
    pct = (price_at_eval - price_at_signal) / price_at_signal

    d = direction.lower()
    if d == "long":
        if pct > flat_threshold_pct:
            return "win", pct
        if pct < -flat_threshold_pct:
            return "loss", pct
        return "flat", pct
    if d == "short":
        if pct < -flat_threshold_pct:
            return "win", pct
        if pct > flat_threshold_pct:
            return "loss", pct
        return "flat", pct
    return "flat", pct


def horizon_label(hours: int) -> str:
    """Human-readable label for a horizon. 24 → '24h', 168 → '7d'."""
    if hours % 24 == 0 and hours >= 24:
        return f"{hours // 24}d"
    return f"{hours}h"
