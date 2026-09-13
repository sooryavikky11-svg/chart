import csv
import lzma
import struct
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from collector.collect import Bar, parse_dukascopy_hour, read_rows, write_bars


class CollectorTests(unittest.TestCase):
    def test_dukascopy_ticks_are_aggregated_to_one_minute(self) -> None:
        records = b"".join(
            (
                struct.pack(">3I2f", 1_000, 110_010, 110_000, 2.0, 3.0),
                struct.pack(">3I2f", 30_000, 110_030, 110_020, 4.0, 5.0),
                struct.pack(">3I2f", 61_000, 110_020, 110_010, 1.0, 2.0),
            )
        )
        bars = parse_dukascopy_hour(
            lzma.compress(records),
            datetime(2024, 1, 2, 12, tzinfo=UTC),
            100_000,
            "EURUSD",
        )

        self.assertEqual(len(bars), 2)
        self.assertAlmostEqual(bars[0].open, 1.10005)
        self.assertAlmostEqual(bars[0].high, 1.10025)
        self.assertAlmostEqual(bars[0].close, 1.10025)
        self.assertAlmostEqual(bars[0].volume, 14.0)

    def test_write_bars_is_idempotent_and_updates_changed_rows(self) -> None:
        timestamp = datetime(2024, 1, 2, 12, 30, tzinfo=UTC)
        original = Bar(timestamp, 10, 12, 9, 11, 5, "test", "ABC")
        changed = Bar(timestamp, 10, 13, 9, 12, 6, "test", "ABC")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(write_bars(root, "abc", [original]), (1, 0, 1))
            self.assertEqual(write_bars(root, "abc", [original]), (0, 0, 0))
            self.assertEqual(write_bars(root, "abc", [changed]), (0, 1, 1))

            path = root / "test" / "abc" / "2024" / "01" / "2024-01-02.csv"
            rows = read_rows(path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(next(iter(rows.values()))["high"], "13")

    def test_written_csv_has_stable_schema(self) -> None:
        bar = Bar(
            datetime(2024, 1, 2, tzinfo=UTC), 10, 11, 9, 10.5, 0, "test", "ABC"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_bars(root, "abc", [bar])
            path = root / "test" / "abc" / "2024" / "01" / "2024-01-02.csv"
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(
                    reader.fieldnames,
                    [
                        "timestamp_utc",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "source",
                        "symbol",
                    ],
                )

    def test_duplicate_input_uses_last_bar_without_false_update(self) -> None:
        timestamp = datetime(2024, 1, 2, 12, 30, tzinfo=UTC)
        original = Bar(timestamp, 10, 12, 9, 11, 5, "test", "ABC")
        temporary = Bar(timestamp, 10, 13, 9, 12, 6, "test", "ABC")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_bars(root, "abc", [original])
            self.assertEqual(
                write_bars(root, "abc", [temporary, original]),
                (0, 0, 0),
            )


if __name__ == "__main__":
    unittest.main()
