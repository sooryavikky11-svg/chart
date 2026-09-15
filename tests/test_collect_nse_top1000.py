import gzip
import tempfile
import unittest
from pathlib import Path

from collector.collect_nse_top1000 import ROW_FIELDS, write_symbol_data


class Top1000CollectorTests(unittest.TestCase):
    def test_symbol_archive_is_gzipped_and_deduplicated(self) -> None:
        original = {
            "timestamp_utc": "2026-09-15T03:45:00Z",
            "open": "100.0",
            "high": "101.0",
            "low": "99.0",
            "close": "100.5",
            "volume": "20.0",
            "symbol": "TEST.NS",
        }
        changed = dict(original, close="100.75")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_symbol_data(root, "TEST", [original])
            write_symbol_data(root, "TEST", [changed])
            path = root / "TEST" / "2026" / "09" / "2026-09-15.csv.gz"
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        self.assertEqual(lines[0].split(","), list(ROW_FIELDS))
        self.assertEqual(len(lines), 2)
        self.assertIn("100.75", lines[1])


if __name__ == "__main__":
    unittest.main()
