"""Tests for the CoinGecko client — no network, uses httpx mock transport."""

import httpx
import pytest

from market_stream.coingecko import ASSET_ID_TO_SYMBOL, CoinGeckoClient, CoinGeckoError


def _mock_transport(handler):
    return httpx.MockTransport(handler)


async def test_fetch_prices_parses_valid_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/simple/price")
        assert "bitcoin,ethereum" in request.url.params["ids"]
        return httpx.Response(
            200,
            json={
                "bitcoin": {"usd": 64123.45, "last_updated_at": 1728000000},
                "ethereum": {"usd": 3421.10, "last_updated_at": 1728000005},
            },
        )

    async with httpx.AsyncClient(transport=_mock_transport(handler)) as http:
        client = CoinGeckoClient(http)
        messages = await client.fetch_prices(["bitcoin", "ethereum"])

    assert len(messages) == 2
    btc = next(m for m in messages if m.asset_id == "bitcoin")
    assert btc.symbol == "BTC"
    assert str(btc.price_usd) == "64123.45"
    assert btc.source_timestamp.tzinfo is not None
    assert btc.ingestion_timestamp.tzinfo is not None


async def test_fetch_prices_skips_missing_asset() -> None:
    """If CoinGecko omits an asset, we log-and-skip rather than raising."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"bitcoin": {"usd": 64000.0, "last_updated_at": 1728000000}},
        )

    async with httpx.AsyncClient(transport=_mock_transport(handler)) as http:
        client = CoinGeckoClient(http)
        messages = await client.fetch_prices(["bitcoin", "ethereum"])

    assert [m.asset_id for m in messages] == ["bitcoin"]


async def test_fetch_prices_skips_malformed_asset() -> None:
    """If an asset's payload is missing required fields, skip it — don't crash."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "bitcoin": {"usd": 64000.0, "last_updated_at": 1728000000},
                "ethereum": {"usd": 3421.10},  # missing last_updated_at
            },
        )

    async with httpx.AsyncClient(transport=_mock_transport(handler)) as http:
        client = CoinGeckoClient(http)
        messages = await client.fetch_prices(["bitcoin", "ethereum"])

    assert [m.asset_id for m in messages] == ["bitcoin"]


async def test_fetch_prices_raises_on_http_error() -> None:
    """5xx and connection errors bubble up as CoinGeckoError."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream unavailable")

    async with httpx.AsyncClient(transport=_mock_transport(handler)) as http:
        client = CoinGeckoClient(http)
        with pytest.raises(CoinGeckoError):
            await client.fetch_prices(["bitcoin"])


def test_symbol_map_covers_configured_assets() -> None:
    """Every asset in the default config has a symbol mapping."""
    from market_stream.config import Settings

    default_assets = Settings().coingecko_asset_ids
    missing = [a for a in default_assets if a not in ASSET_ID_TO_SYMBOL]
    assert not missing, f"missing symbol mapping for: {missing}"