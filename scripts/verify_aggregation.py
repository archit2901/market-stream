from datetime import datetime, timedelta, timezone
from decimal import Decimal

from market_stream.aggregation import AggregatorState
from market_stream.schemas import PriceTick

T0 = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)


def make_tick(minute: int, price: str) -> PriceTick:
    ts = T0 + timedelta(minutes=minute)
    return PriceTick(
        symbol="BTC",
        asset_class="crypto",
        price_usd=Decimal(price),
        source="coingecko",
        source_timestamp=ts,
        ingestion_timestamp=ts,
        normalized_timestamp=ts,
    )


def main() -> None:
    state = AggregatorState()

    # One tick per minute for 70 minutes. Price climbs $1/minute from 100 to 170,
    # except minute 68, which dips to 150.
    snapshot = None
    for minute in range(71):
        price = "150" if minute == 68 else str(100 + minute)
        snapshot = state.apply(make_tick(minute, price))

    assert snapshot is not None
    print(f"latest price: {snapshot.latest_price} at {snapshot.latest_source_timestamp.isoformat()}")
    for name, w in snapshot.windows.items():
        print(
            f"{name}: open={w.open_price} high={w.high} low={w.low} "
            f"change={w.change_pct}% ticks={w.tick_count}"
        )

    print("duplicate ignored:", state.apply(make_tick(70, "999")) is None)
    print("older tick ignored:", state.apply(make_tick(50, "999")) is None)


if __name__ == "__main__":
    main()