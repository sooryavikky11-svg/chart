#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import lzma
import os
import struct
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


USER_AGENT = "market-data-archiver/1.0 (+https://github.com/sooryavikky11-svg/chart)"
CSV_FIELDS = (
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "symbol",
)


@dataclass(frozen=True)
class Bar:
    timestamp_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    symbol: str

    def validate(self) -> None:
        if self.timestamp_utc.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        if self.high < max(self.open, self.close, self.low):
            raise ValueError(f"invalid high at {self.timestamp_utc.isoformat()}")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError(f"invalid low at {self.timestamp_utc.isoformat()}")
        if self.volume < 0:
            raise ValueError(f"negative volume at {self.timestamp_utc.isoformat()}")


@dataclass
class FeedResult:
    provider: str
    symbol: str
    name: str
    status: str
    bars_fetched: int = 0
    bars_inserted: int = 0
    bars_updated: int = 0
    files_written: int = 0
    earliest_timestamp: str | None = None
    latest_timestamp: str | None = None
    duration_seconds: float = 0
    error: str | None = None


def request_bytes(url: str, attempts: int = 4) -> bytes:
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=45) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                raise
            time.sleep(2**attempt)
        except (URLError, TimeoutError, ConnectionError):
            if attempt == attempts - 1:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("request retry loop exited unexpectedly")


def fetch_yahoo(feed: dict[str, Any]) -> list[Bar]:
    symbol = str(feed["symbol"])
    interval = str(feed.get("interval", "1m"))
    period = str(feed.get("range", "7d"))
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{quote(symbol, safe='')}?interval={interval}&range={period}"
    )
    payload = json.loads(request_bytes(url))
    chart = payload.get("chart", {})
    if chart.get("error"):
        raise RuntimeError(f"Yahoo error: {chart['error']}")
    results = chart.get("result") or []
    if not results:
        raise RuntimeError("Yahoo returned no chart result")

    result = results[0]
    timestamps = result.get("timestamp") or []
    quotes = ((result.get("indicators") or {}).get("quote") or [])
    if not timestamps or not quotes:
        raise RuntimeError("Yahoo returned no candle data")

    values = quotes[0]
    bars: list[Bar] = []
    for index, epoch in enumerate(timestamps):
        candle = {
            key: (values.get(key) or [])[index]
            for key in ("open", "high", "low", "close")
        }
        if any(value is None for value in candle.values()):
            continue
        volumes = values.get("volume") or []
        volume = volumes[index] if index < len(volumes) and volumes[index] is not None else 0
        bar = Bar(
            timestamp_utc=datetime.fromtimestamp(epoch, UTC).replace(second=0, microsecond=0),
            open=float(candle["open"]),
            high=float(candle["high"]),
            low=float(candle["low"]),
            close=float(candle["close"]),
            volume=float(volume),
            source="yahoo",
            symbol=symbol,
        )
        bar.validate()
        bars.append(bar)
    if not bars:
        raise RuntimeError("Yahoo returned no valid candles")
    return bars


def parse_dukascopy_hour(
    compressed: bytes,
    hour_start: datetime,
    price_scale: int,
    symbol: str,
) -> list[Bar]:
    raw = lzma.decompress(compressed)
    record_size = struct.calcsize(">3I2f")
    if len(raw) % record_size:
        raise ValueError(f"Dukascopy payload has {len(raw)} bytes, not a multiple of {record_size}")

    minutes: dict[datetime, list[tuple[float, float]]] = defaultdict(list)
    for offset in range(0, len(raw), record_size):
        milliseconds, ask, bid, ask_volume, bid_volume = struct.unpack(
            ">3I2f", raw[offset : offset + record_size]
        )
        timestamp = (hour_start + timedelta(milliseconds=milliseconds)).replace(
            second=0, microsecond=0
        )
        midpoint = ((ask / price_scale) + (bid / price_scale)) / 2
        minutes[timestamp].append((midpoint, float(ask_volume + bid_volume)))

    bars: list[Bar] = []
    for timestamp, ticks in sorted(minutes.items()):
        prices = [tick[0] for tick in ticks]
        bar = Bar(
            timestamp_utc=timestamp,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            volume=sum(tick[1] for tick in ticks),
            source="dukascopy",
            symbol=symbol,
        )
        bar.validate()
        bars.append(bar)
    return bars


def fetch_dukascopy(feed: dict[str, Any], now: datetime) -> list[Bar]:
    symbol = str(feed["symbol"]).upper()
    price_scale = int(feed["price_scale"])
    days_back = int(feed.get("days_back", 3))
    bars: list[Bar] = []
    successful_hours = 0
    end_date = now.astimezone(UTC).date()

    for days_ago in range(days_back, 0, -1):
        target = end_date - timedelta(days=days_ago)
        for hour in range(24):
            # Dukascopy uses a zero-based month in its datafeed path.
            url = (
                f"https://datafeed.dukascopy.com/datafeed/{symbol}/"
                f"{target.year:04d}/{target.month - 1:02d}/{target.day:02d}/"
                f"{hour:02d}h_ticks.bi5"
            )
            try:
                compressed = request_bytes(url)
            except HTTPError as exc:
                if exc.code == 404:
                    continue
                raise
            if not compressed:
                continue
            hour_start = datetime(
                target.year, target.month, target.day, hour, tzinfo=UTC
            )
            bars.extend(parse_dukascopy_hour(compressed, hour_start, price_scale, symbol))
            successful_hours += 1

    if successful_hours == 0 or not bars:
        raise RuntimeError(
            f"Dukascopy returned no tick data for {symbol} over {days_back} completed days"
        )
    return bars


def bar_to_row(bar: Bar) -> dict[str, str]:
    return {
        "timestamp_utc": bar.timestamp_utc.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "open": repr(bar.open),
        "high": repr(bar.high),
        "low": repr(bar.low),
        "close": repr(bar.close),
        "volume": repr(bar.volume),
        "source": bar.source,
        "symbol": bar.symbol,
    }


def read_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CSV_FIELDS:
            raise ValueError(f"{path} has an unexpected CSV schema")
        return {row["timestamp_utc"]: row for row in reader}


def write_bars(data_root: Path, name: str, bars: Iterable[Bar]) -> tuple[int, int, int]:
    grouped: dict[date, list[Bar]] = defaultdict(list)
    for bar in bars:
        grouped[bar.timestamp_utc.astimezone(UTC).date()].append(bar)

    inserted = 0
    updated = 0
    files_written = 0
    for day, day_bars in sorted(grouped.items()):
        source = day_bars[0].source
        path = (
            data_root
            / source
            / name
            / f"{day.year:04d}"
            / f"{day.month:02d}"
            / f"{day.isoformat()}.csv"
        )
        existing = read_rows(path)
        before = dict(existing)
        incoming: dict[str, dict[str, str]] = {}
        for bar in day_bars:
            row = bar_to_row(bar)
            incoming[row["timestamp_utc"]] = row

        inserted += sum(key not in before for key in incoming)
        updated += sum(
            key in before and before[key] != row for key, row in incoming.items()
        )
        existing.update(incoming)

        if existing == before:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(existing[key] for key in sorted(existing))
        os.replace(temporary, path)
        files_written += 1
    return inserted, updated, files_written


def collect_feed(feed: dict[str, Any], data_root: Path, now: datetime) -> FeedResult:
    started = time.monotonic()
    result = FeedResult(
        provider=str(feed["provider"]),
        symbol=str(feed["symbol"]),
        name=str(feed["name"]),
        status="failed",
    )
    try:
        if result.provider == "yahoo":
            bars = fetch_yahoo(feed)
        elif result.provider == "dukascopy":
            bars = fetch_dukascopy(feed, now)
        else:
            raise ValueError(f"unsupported provider: {result.provider}")

        inserted, updated, files_written = write_bars(data_root, result.name, bars)
        result.status = "success"
        result.bars_fetched = len(bars)
        result.bars_inserted = inserted
        result.bars_updated = updated
        result.files_written = files_written
        result.earliest_timestamp = min(bar.timestamp_utc for bar in bars).isoformat()
        result.latest_timestamp = max(bar.timestamp_utc for bar in bars).isoformat()
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
    finally:
        result.duration_seconds = round(time.monotonic() - started, 3)
    return result


def write_run_log(log_root: Path, now: datetime, results: list[FeedResult]) -> Path:
    log_root.mkdir(parents=True, exist_ok=True)
    failures = sum(result.status != "success" for result in results)
    payload = {
        "run_started_utc": now.astimezone(UTC).isoformat(),
        "run_completed_utc": datetime.now(UTC).isoformat(),
        "status": "success" if failures == 0 else "partial_failure",
        "feed_count": len(results),
        "successful_feeds": len(results) - failures,
        "failed_feeds": failures,
        "totals": {
            "bars_fetched": sum(result.bars_fetched for result in results),
            "bars_inserted": sum(result.bars_inserted for result in results),
            "bars_updated": sum(result.bars_updated for result in results),
            "files_written": sum(result.files_written for result in results),
        },
        "feeds": [asdict(result) for result in results],
    }
    stamp = now.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    run_path = log_root / "runs" / f"{stamp}.json"
    run_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    run_path.write_text(text, encoding="utf-8")
    (log_root / "latest.json").write_text(text, encoding="utf-8")
    return run_path


def parse_args() -> argparse.Namespace:
    repository_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Collect and archive one-minute market data")
    parser.add_argument(
        "--config",
        type=Path,
        default=repository_root / "collector" / "config.json",
    )
    parser.add_argument("--data-root", type=Path, default=repository_root / "data")
    parser.add_argument("--log-root", type=Path, default=repository_root / "logs")
    parser.add_argument("--workers", type=int, default=6)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now(UTC)
    config = json.loads(args.config.read_text(encoding="utf-8"))
    feeds = config.get("feeds")
    if not isinstance(feeds, list) or not feeds:
        raise ValueError("configuration must contain a non-empty feeds list")

    results: list[FeedResult] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(collect_feed, feed, args.data_root, now): feed
            for feed in feeds
        }
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda result: (result.provider, result.name))
    log_path = write_run_log(args.log_root, now, results)
    for result in results:
        print(
            f"{result.status.upper():15} {result.provider:10} {result.name:20} "
            f"fetched={result.bars_fetched} inserted={result.bars_inserted} "
            f"updated={result.bars_updated}"
        )
        if result.error:
            print(f"  {result.error}", file=sys.stderr)
    print(f"Audit log: {log_path}")
    return 1 if any(result.status != "success" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
