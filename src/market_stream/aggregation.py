from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from market_stream.schemas import AssetClass, AssetSnapshot, PriceTick, WindowStats

WINDOWS: dict[str, timedelta] = {
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
}
MAX_WINDOW = max(WINDOWS.values())
_PCT_PLACES = Decimal("0.0001")  # round percent change to 4 decimal places


@dataclass(frozen=True)
class _Point:
    ts: datetime
    price: Decimal


class AssetWindow:
    """The last hour of ticks for ONE asset, ordered by event time."""

    def __init__(self, symbol: str, asset_class: AssetClass) -> None:
        self.symbol = symbol
        self.asset_class: AssetClass = asset_class
        self._points: deque[_Point] = deque()
        self._is_stale = False

    def add(self, tick: PriceTick) -> bool:
        """Add a tick. Returns False (and ignores it) if it isn't newer than the latest one."""
        if self._points and tick.source_timestamp <= self._points[-1].ts:
            return False

        self._points.append(_Point(tick.source_timestamp, tick.price_usd))
        self._is_stale = tick.is_stale

        # Trim: drop anything more than an hour older than the newest tick.
        cutoff = tick.source_timestamp - MAX_WINDOW
        while self._points[0].ts < cutoff:
            self._points.popleft()
        return True

    def snapshot(self) -> AssetSnapshot:
        latest = self._points[-1]
        return AssetSnapshot(
            symbol=self.symbol,
            asset_class=self.asset_class,
            latest_price=latest.price,
            latest_source_timestamp=latest.ts,
            is_stale=self._is_stale,
            windows={
                name: self._window_stats(window_start=latest.ts - length)
                for name, length in WINDOWS.items()
            },
        )

    def _window_stats(self, window_start: datetime) -> WindowStats:
        # Never empty: the latest tick is always inside every window.
        prices = [p.price for p in self._points if p.ts >= window_start]
        open_price, latest_price = prices[0], prices[-1]

        if open_price == 0:
            change = Decimal("0")
        else:
            change = ((latest_price - open_price) / open_price * 100).quantize(_PCT_PLACES)

        return WindowStats(
            open_price=open_price,
            high=max(prices),
            low=min(prices),
            change_pct=change,
            tick_count=len(prices),
        )


class AggregatorState:
    """Rolling windows for every asset. apply() is the only way in."""

    def __init__(self) -> None:
        self._assets: dict[tuple[AssetClass, str], AssetWindow] = {}

    def apply(self, tick: PriceTick) -> AssetSnapshot | None:
        """Feed one tick. Returns the asset's updated snapshot, or None if the tick was ignored."""
        key = (tick.asset_class, tick.symbol)
        window = self._assets.get(key)
        if window is None:
            window = AssetWindow(tick.symbol, tick.asset_class)
            self._assets[key] = window

        if not window.add(tick):
            return None
        return window.snapshot()