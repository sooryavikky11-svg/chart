#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import html
import io
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


USER_AGENT = "market-data-archiver/1.0 (+https://github.com/sooryavikky11-svg/chart)"
NSE_SYMBOLS_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_SYMBOLS_CACHE = Path(__file__).with_name("universe") / "EQUITY_L.csv"
SCREENER_URL = "https://www.screener.in/screens/29729/top-1000-stocks/?page={page}"
YAHOO_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/"
    "{symbol}?interval=1m&range=7d"
)
ROW_FIELDS = (
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "symbol",
)


@dataclass
class SymbolResult:
    rank: int
    nse_symbol: str
    yahoo_symbol: str
    status: str
    bars: int = 0
    earliest_timestamp: str | None = None
    latest_timestamp: str | None = None
    error: str | None = None


def request_bytes(url: str, attempts: int = 5) -> bytes:
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=45) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                raise
        except (URLError, TimeoutError, ConnectionError):
            if attempt == attempts - 1:
                raise
        time.sleep(min(30, 2**attempt))
    raise RuntimeError("request retry loop exited unexpectedly")


def fetch_nse_symbols() -> set[str]:
    if NSE_SYMBOLS_CACHE.exists():
        text = NSE_SYMBOLS_CACHE.read_text(encoding="utf-8-sig")
    else:
        text = request_bytes(NSE_SYMBOLS_URL).decode("utf-8-sig")
        NSE_SYMBOLS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        NSE_SYMBOLS_CACHE.write_text(text, encoding="utf-8")
    rows = csv.DictReader(StringIO(text))
    return {
        row["SYMBOL"].strip()
        for row in rows
        if row.get("SYMBOL") and row.get(" SERIES", row.get("SERIES", "")).strip() == "EQ"
    }


def fetch_top_symbols(limit: int) -> list[str]:
    nse_symbols = fetch_nse_symbols()
    ranked: list[str] = []
    seen: set[str] = set()
    page = 1

    while len(ranked) < limit and page <= 100:
        text = request_bytes(SCREENER_URL.format(page=page)).decode("utf-8")
        candidates = re.findall(r'href="/company/([^/]+)/', text)
        if not candidates:
            raise RuntimeError(f"Screener page {page} contained no company symbols")
        for candidate in candidates:
            symbol = html.unescape(candidate).strip()
            if symbol in nse_symbols and symbol not in seen:
                seen.add(symbol)
                ranked.append(symbol)
                if len(ranked) == limit:
                    break
        page += 1

    if len(ranked) != limit:
        raise RuntimeError(
            f"only {len(ranked)} ranked symbols matched the NSE equity master"
        )
    return ranked


def fetch_symbol(rank: int, symbol: str) -> tuple[SymbolResult, list[dict[str, str]]]:
    yahoo_symbol = f"{symbol}.NS"
    result = SymbolResult(rank, symbol, yahoo_symbol, "failed")
    try:
        payload = json.loads(
            request_bytes(YAHOO_URL.format(symbol=quote(yahoo_symbol, safe="")))
        )
        chart = payload.get("chart", {})
        if chart.get("error"):
            raise RuntimeError(f"Yahoo error: {chart['error']}")
        charts = chart.get("result") or []
        if not charts:
            raise RuntimeError("Yahoo returned no chart result")
        chart_result = charts[0]
        timestamps = chart_result.get("timestamp") or []
        quotes = ((chart_result.get("indicators") or {}).get("quote") or [])
        if not timestamps or not quotes:
            raise RuntimeError("Yahoo returned no candle data")
        values = quotes[0]
        rows: list[dict[str, str]] = []
        for index, epoch in enumerate(timestamps):
            candle: dict[str, Any] = {
                key: (values.get(key) or [])[index]
                for key in ("open", "high", "low", "close")
            }
            if any(value is None for value in candle.values()):
                continue
            volumes = values.get("volume") or []
            volume = (
                volumes[index]
                if index < len(volumes) and volumes[index] is not None
                else 0
            )
            timestamp = datetime.fromtimestamp(epoch, UTC).replace(
                second=0, microsecond=0
            )
            rows.append(
                {
                    "timestamp_utc": timestamp.isoformat().replace("+00:00", "Z"),
                    "open": repr(float(candle["open"])),
                    "high": repr(float(candle["high"])),
                    "low": repr(float(candle["low"])),
                    "close": repr(float(candle["close"])),
                    "volume": repr(float(volume)),
                    "symbol": yahoo_symbol,
                }
            )
        if not rows:
            raise RuntimeError("Yahoo returned no valid candles")
        result.status = "success"
        result.bars = len(rows)
        result.earliest_timestamp = rows[0]["timestamp_utc"]
        result.latest_timestamp = rows[-1]["timestamp_utc"]
        return result, rows
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        return result, []


def write_symbol_data(root: Path, symbol: str, rows: list[dict[str, str]]) -> None:
    by_date: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        day = row["timestamp_utc"][:10]
        by_date.setdefault(day, []).append(row)

    for day, daily_rows in by_date.items():
        year, month, _ = day.split("-")
        path = root / symbol / year / month / f"{day}.csv.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        existing: dict[str, dict[str, str]] = {}
        if path.exists():
            with gzip.open(path, "rt", newline="", encoding="utf-8") as handle:
                existing = {
                    row["timestamp_utc"]: row for row in csv.DictReader(handle)
                }
        updated = dict(existing)
        for row in daily_rows:
            updated[row["timestamp_utc"]] = row
        if updated == existing:
            continue

        temporary = path.with_suffix(".tmp")
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
                with io.TextIOWrapper(
                    compressed, encoding="utf-8", newline=""
                ) as handle:
                    writer = csv.DictWriter(handle, fieldnames=ROW_FIELDS)
                    writer.writeheader()
                    writer.writerows(updated[key] for key in sorted(updated))
        temporary.replace(path)


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--data-root", type=Path, default=root / "data" / "yahoo" / "nse_top1000")
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=root / "logs" / "nse_top1000",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = datetime.now(UTC)
    symbols = fetch_top_symbols(args.limit)
    results: list[SymbolResult] = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(fetch_symbol, rank, symbol): (rank, symbol)
            for rank, symbol in enumerate(symbols, 1)
        }
        for completed, future in enumerate(as_completed(futures), 1):
            result, rows = future.result()
            results.append(result)
            if rows:
                write_symbol_data(args.data_root, result.nse_symbol, rows)
            print(
                f"[{completed:04d}/{args.limit}] {result.status.upper():7} "
                f"rank={result.rank:04d} {result.nse_symbol} bars={result.bars}",
                flush=True,
            )

    results.sort(key=lambda item: item.rank)
    failures = [result for result in results if result.status != "success"]
    payload = {
        "run_started_utc": started.isoformat(),
        "run_completed_utc": datetime.now(UTC).isoformat(),
        "requested_symbols": args.limit,
        "successful_symbols": len(results) - len(failures),
        "failed_symbols": len(failures),
        "total_bars": sum(result.bars for result in results),
        "universe_method": (
            "Current Screener top-stocks ranking sorted by market capitalization, "
            "intersected with the official NSE EQ-series symbol master."
        ),
        "results": [asdict(result) for result in results],
    }
    args.manifest_root.mkdir(parents=True, exist_ok=True)
    stamp = started.strftime("%Y-%m-%dT%H-%M-%SZ")
    manifest = args.manifest_root / f"{stamp}.json"
    text = json.dumps(payload, indent=2) + "\n"
    manifest.write_text(text, encoding="utf-8")
    (args.manifest_root / "latest.json").write_text(text, encoding="utf-8")
    print(
        f"Completed: {payload['successful_symbols']}/{args.limit} symbols, "
        f"{payload['total_bars']} bars. Manifest: {manifest}"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
