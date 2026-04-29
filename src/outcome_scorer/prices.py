"""Historical price feeds.

Strategy: for any (asset, timestamp) pair, return the asset's close price at the
closest 1-hour bar. Crypto via Binance public klines (no key); US equities via
Finnhub /stock/candle, with Yahoo Finance chart API as fallback when Finnhub
is unavailable (free-tier 403 on Finnhub for some symbols/historical depth).
Anything else returns None.
"""
import logging
from datetime import datetime, timedelta

import httpx

from outcome_scorer.settings import settings

log = logging.getLogger(__name__)

CRYPTO = {"BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "MATIC", "DOT"}
US_EQUITIES = {"NVDA", "AAPL", "MSFT", "GOOG", "GOOGL", "META", "AMZN", "TSLA", "AMD"}

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
# Yahoo's chart API rejects requests without a User-Agent
YAHOO_USER_AGENT = "Mozilla/5.0 (outcome-scorer/0.1)"


async def price_at(asset: str, ts: datetime) -> float | None:
    """Return the close price of the 1h bar containing ts.

    None if asset isn't supported or the API has no data for that timestamp.
    For US equities, tries Finnhub first (preferred — has a key, no UA hassles),
    falls back to Yahoo's free chart API on any failure.
    """
    a = asset.upper()
    try:
        if a in CRYPTO:
            return await _binance_close_at(a, ts)
        if a in US_EQUITIES:
            price = await _finnhub_close_at(a, ts)
            if price is not None:
                return price
            log.debug("Finnhub returned no price for %s @ %s — trying Yahoo", a, ts)
            return await _yahoo_close_at(a, ts)
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
    """Fetch a single 1h candle for the given symbol containing ts.

    US equities only trade during RTH (9:30-16:00 ET, weekdays). Signals that
    fire outside those hours have no in-window candle, so we fall back to the
    nearest trading-hour close (±96h covers a full long-weekend gap).

    Returns None if Finnhub is unavailable or returns nothing — the caller
    falls back to Yahoo's chart API.
    """
    if not settings.finnhub_key:
        return None
    start = int(ts.timestamp())
    end = int((ts + timedelta(hours=1)).timestamp())
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(
                f"{settings.finnhub_base_url}/stock/candle",
                params={"symbol": symbol, "resolution": 60, "from": start,
                        "to": end, "token": settings.finnhub_key},
            )
            if r.status_code == 403:
                # Free-tier Finnhub returns 403 for some symbols / depths.
                # Return None so price_at() falls through to Yahoo.
                log.debug("Finnhub 403 for %s — caller will try Yahoo", symbol)
                return None
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPError as exc:
        log.debug("Finnhub HTTP error for %s: %s — caller will try Yahoo", symbol, exc)
        return None

    closes = data.get("c") or []
    if data.get("s") == "ok" and closes:
        return float(closes[0])
    # Either status not ok, or empty closes (both happen outside RTH) → fall back
    return await _finnhub_close_nearest(symbol, ts)


async def _finnhub_close_nearest(symbol: str, ts: datetime) -> float | None:
    """Fallback: widen window to ±96h so we get the nearest RTH close (covers long weekends)."""
    start = int((ts - timedelta(hours=96)).timestamp())
    end = int((ts + timedelta(hours=96)).timestamp())
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(
                f"{settings.finnhub_base_url}/stock/candle",
                params={"symbol": symbol, "resolution": 60, "from": start,
                        "to": end, "token": settings.finnhub_key},
            )
            if r.status_code == 403:
                return None
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPError as exc:
        log.debug("Finnhub nearest HTTP error for %s: %s", symbol, exc)
        return None

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


async def _yahoo_close_at(
    symbol: str, ts: datetime,
    *,
    client: httpx.AsyncClient | None = None,
) -> float | None:
    """Fetch a close price from Yahoo's chart API (no auth, no key, no SDK).

    Endpoint: /v8/finance/chart/<symbol>?interval=1h&period1=<unix>&period2=<unix>
    Yahoo accepts a wide window — we ask ±96h to cover weekends + outside-RTH
    signals, then pick the nearest bar to ts.

    Yahoo rejects python-requests/* User-Agents with a 429 — we use a Mozilla
    string. No documented rate limit; informal observations suggest several
    thousand requests per hour are tolerated.

    Args:
        client: optional httpx.AsyncClient (used by tests with MockTransport).
            When omitted, a fresh client is created and closed inside.
    """
    start = int((ts - timedelta(hours=96)).timestamp())
    end = int((ts + timedelta(hours=96)).timestamp())

    own_client = client is None
    c = client or httpx.AsyncClient(
        timeout=15.0, headers={"User-Agent": YAHOO_USER_AGENT},
    )
    try:
        r = await c.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={"interval": "1h", "period1": start, "period2": end},
        )
        r.raise_for_status()
        payload = r.json()
    except httpx.HTTPError as exc:
        log.warning("Yahoo HTTP error for %s @ %s: %s", symbol, ts, exc)
        return None
    except ValueError as exc:
        log.warning("Yahoo non-JSON for %s: %s", symbol, exc)
        return None
    finally:
        if own_client:
            await c.aclose()

    chart = payload.get("chart", {})
    if chart.get("error"):
        log.warning("Yahoo chart error for %s: %s", symbol, chart["error"])
        return None
    results = chart.get("result") or []
    if not results:
        log.debug("Yahoo: no chart results for %s @ %s", symbol, ts)
        return None
    res = results[0]
    timestamps = res.get("timestamp") or []
    quote = (res.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    if not timestamps or not closes:
        return None

    target = ts.timestamp()
    # nearest non-None close by abs delta
    pairs = [(t, c) for t, c in zip(timestamps, closes, strict=False) if c is not None]
    if not pairs:
        return None
    nearest = min(pairs, key=lambda tc: abs(tc[0] - target))
    return float(nearest[1])
