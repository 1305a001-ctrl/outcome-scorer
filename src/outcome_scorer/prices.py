"""Historical price feeds.

Strategy: for any (asset, timestamp) pair, return the asset's close price at the
closest 1-hour bar. Crypto via Binance public klines (no key); US equities via
Finnhub /stock/candle. Anything else returns None.
"""
import logging
from datetime import datetime, timedelta

import httpx

from outcome_scorer.settings import settings

log = logging.getLogger(__name__)

CRYPTO = {"BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "MATIC", "DOT"}
US_EQUITIES = {"NVDA", "AAPL", "MSFT", "GOOG", "GOOGL", "META", "AMZN", "TSLA", "AMD"}


async def price_at(asset: str, ts: datetime) -> float | None:
    """Return the close price of the 1h bar containing ts.

    None if asset isn't supported or the API has no data for that timestamp.
    """
    a = asset.upper()
    try:
        if a in CRYPTO:
            return await _binance_close_at(a, ts)
        if a in US_EQUITIES:
            return await _finnhub_close_at(a, ts)
        log.debug("No price feed for asset %s", asset)
        return None
    except Exception as exc:  # noqa: BLE001
        log.error("Price fetch failed for %s @ %s: %s", asset, ts, exc)
        return None


async def _binance_close_at(symbol: str, ts: datetime) -> float | None:
    """Fetch a single 1h candle covering ts. Binance: /api/v3/klines."""
    pair = f"{symbol}USDT"
    start_ms = int(ts.timestamp() * 1000)
    end_ms = int((ts + timedelta(hours=1)).timestamp() * 1000)
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.get(
            f"{settings.binance_base_url}/api/v3/klines",
            params={"symbol": pair, "interval": "1h", "startTime": start_ms,
                    "endTime": end_ms, "limit": 1},
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return None
        # kline row: [openTime, open, high, low, close, volume, closeTime, ...]
        return float(rows[0][4])


async def _finnhub_close_at(symbol: str, ts: datetime) -> float | None:
    """Fetch a single 1h candle for the given symbol containing ts."""
    if not settings.finnhub_key:
        return None
    start = int(ts.timestamp())
    end = int((ts + timedelta(hours=1)).timestamp())
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.get(
            f"{settings.finnhub_base_url}/stock/candle",
            params={"symbol": symbol, "resolution": 60, "from": start,
                    "to": end, "token": settings.finnhub_key},
        )
        r.raise_for_status()
        data = r.json()
        if data.get("s") != "ok":
            # 'no_data' is normal outside RTH — caller should fall back to a wider window
            return await _finnhub_close_nearest(symbol, ts)
        closes = data.get("c") or []
        return float(closes[0]) if closes else None


async def _finnhub_close_nearest(symbol: str, ts: datetime) -> float | None:
    """Fallback: widen the window to ±48h so we get the nearest available trading-hour close."""
    start = int((ts - timedelta(hours=48)).timestamp())
    end = int((ts + timedelta(hours=48)).timestamp())
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.get(
            f"{settings.finnhub_base_url}/stock/candle",
            params={"symbol": symbol, "resolution": 60, "from": start,
                    "to": end, "token": settings.finnhub_key},
        )
        r.raise_for_status()
        data = r.json()
        if data.get("s") != "ok":
            return None
        timestamps = data.get("t") or []
        closes = data.get("c") or []
        if not timestamps:
            return None
        target = ts.timestamp()
        # nearest by abs delta
        idx, _ = min(enumerate(timestamps), key=lambda kv: abs(kv[1] - target))
        return float(closes[idx])
