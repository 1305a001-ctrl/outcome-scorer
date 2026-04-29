"""Tests for prices.price_at — Finnhub primary, Yahoo fallback."""
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from outcome_scorer import prices
from outcome_scorer.prices import _yahoo_close_at, price_at


def _ts() -> datetime:
    return datetime(2026, 4, 28, 14, 0, tzinfo=UTC)


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ─── Yahoo fallback ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_yahoo_returns_nearest_close_to_target_ts():
    target = _ts()
    target_unix = int(target.timestamp())
    payload = {
        "chart": {
            "result": [{
                "timestamp": [target_unix - 7200, target_unix - 600, target_unix + 3600],
                "indicators": {"quote": [{"close": [100.0, 105.5, 110.0]}]},
            }],
            "error": None,
        }
    }
    async with _mock_client(lambda req: httpx.Response(200, json=payload)) as c:
        price = await _yahoo_close_at("NVDA", target, client=c)
    # -600s (10 min before target) is closest
    assert price == 105.5


@pytest.mark.asyncio
async def test_yahoo_skips_none_closes():
    """Yahoo returns null closes for hours with no trade — skip them."""
    target = _ts()
    target_unix = int(target.timestamp())
    payload = {
        "chart": {
            "result": [{
                "timestamp": [target_unix - 600, target_unix + 600],
                "indicators": {"quote": [{"close": [None, 200.0]}]},
            }],
            "error": None,
        }
    }
    async with _mock_client(lambda req: httpx.Response(200, json=payload)) as c:
        price = await _yahoo_close_at("NVDA", target, client=c)
    # First (closer) bar has None close, skip → second is the only valid one
    assert price == 200.0


@pytest.mark.asyncio
async def test_yahoo_returns_none_on_error_field():
    payload = {"chart": {"result": None, "error": {"code": "Not Found"}}}
    async with _mock_client(lambda req: httpx.Response(200, json=payload)) as c:
        price = await _yahoo_close_at("BOGUS", _ts(), client=c)
    assert price is None


@pytest.mark.asyncio
async def test_yahoo_returns_none_on_http_error():
    async with _mock_client(lambda req: httpx.Response(429, text="rate limited")) as c:
        price = await _yahoo_close_at("NVDA", _ts(), client=c)
    assert price is None


@pytest.mark.asyncio
async def test_yahoo_returns_none_on_empty_result():
    payload = {"chart": {"result": [], "error": None}}
    async with _mock_client(lambda req: httpx.Response(200, json=payload)) as c:
        price = await _yahoo_close_at("NVDA", _ts(), client=c)
    assert price is None


# ─── price_at orchestration ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_price_at_returns_none_for_unknown_asset():
    p = await price_at("XYZ-UNKNOWN", _ts())
    assert p is None


@pytest.mark.asyncio
async def test_price_at_falls_back_to_yahoo_when_finnhub_returns_none():
    """If Finnhub gives None (403 free tier), Yahoo must be tried."""
    with patch.object(prices, "_finnhub_close_at", new=AsyncMock(return_value=None)) as fhub, \
         patch.object(prices, "_yahoo_close_at", new=AsyncMock(return_value=42.5)) as yahoo:
        price = await price_at("NVDA", _ts())
    fhub.assert_awaited_once()
    yahoo.assert_awaited_once()
    assert price == 42.5


@pytest.mark.asyncio
async def test_price_at_uses_finnhub_when_available():
    """Finnhub returning a valid price means Yahoo is not called."""
    with patch.object(prices, "_finnhub_close_at", new=AsyncMock(return_value=99.99)) as fhub, \
         patch.object(prices, "_yahoo_close_at", new=AsyncMock(return_value=42.5)) as yahoo:
        price = await price_at("NVDA", _ts())
    fhub.assert_awaited_once()
    yahoo.assert_not_called()
    assert price == 99.99


@pytest.mark.asyncio
async def test_price_at_returns_none_when_both_fail():
    with patch.object(prices, "_finnhub_close_at", new=AsyncMock(return_value=None)), \
         patch.object(prices, "_yahoo_close_at", new=AsyncMock(return_value=None)):
        price = await price_at("NVDA", _ts())
    assert price is None
