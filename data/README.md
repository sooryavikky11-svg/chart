# Archived market data

Files are partitioned by provider, instrument, year, month, and UTC date:

```text
data/<provider>/<instrument>/<year>/<month>/<yyyy-mm-dd>.csv
```

Each file contains one-minute OHLCV bars with UTC timestamps. Providers remain
separate because their prices, market coverage, and volume definitions differ.

