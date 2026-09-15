# Market data archive

This repository collects one-minute market data once per day and stores it for
future strategy backtesting.

## Feeds

| Provider | Instrument | Provider symbol |
| --- | --- | --- |
| Yahoo Finance | NIFTY 50 spot index | `^NSEI` |
| Yahoo Finance | Gold futures | `GC=F` |
| Yahoo Finance | EUR/USD | `EURUSD=X` |
| Yahoo Finance | USD/INR | `USDINR=X` |
| Dukascopy | EUR/USD | `EURUSD` |
| Dukascopy | Gold spot | `XAUUSD` |
| Yahoo Finance | Top 1,000 NSE equities by market capitalization | `<NSE symbol>.NS` |

Yahoo downloads its rolling seven-day one-minute window on every run, allowing
the collector to recover from several missed days. Dukascopy downloads the last
three completed UTC days from hourly tick files and aggregates midpoint prices
into one-minute bars.

Data from different providers is never merged. Every CSV row includes its source
and symbol, timestamps are normalized to UTC, and repeated collection runs
deduplicate by timestamp.

The top-1,000 universe is rebuilt from Screener's descending market-cap ranking
and intersected with the official NSE EQ-series symbol master. Its compressed
one-minute files are partitioned by symbol and UTC date under
`data/yahoo/nse_top1000/`. A complete per-symbol audit manifest is stored under
`logs/nse_top1000/`.

## Automation

[`.github/workflows/collect-market-data.yml`](.github/workflows/collect-market-data.yml)
runs daily at 23:30 UTC and can also be started manually from the Actions tab.
It uses the repository's built-in `GITHUB_TOKEN`; no personal access token is
required.

The workflow:

1. Runs the unit tests.
2. Fetches all feeds concurrently.
3. Validates and stores daily CSV partitions.
4. Writes a detailed JSON audit log.
5. Commits and pushes new data and logs.
6. Fails visibly if any feed failed, after preserving successful results.

## Run locally

Python 3.11 or newer is required. No third-party packages are used.

```bash
python3 -m unittest discover -s tests -v
python3 collector/collect.py
```

The collector may exit with status 1 when one provider is unavailable. Review
`logs/latest.json` for the status of every feed.

## Data caveats

- Yahoo and Dukascopy are third-party sources and are not exchange-grade.
- `^NSEI` is the NIFTY spot index, not NIFTY futures.
- `GC=F` is a rolling Yahoo futures symbol; contract rollover can create gaps.
- Dukascopy bars use tick midpoint prices and summed tick volume.
- Confirm each provider's terms before redistributing or commercially using data.
- Git repositories are not ideal for unlimited high-frequency data. If repository
  size becomes material, migrate historical partitions to object storage.
