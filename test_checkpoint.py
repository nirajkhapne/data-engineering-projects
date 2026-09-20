import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from producer.checkpoint import get_checkpoint, update_checkpoint


class CheckpointTests(unittest.TestCase):
    def test_missing_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            ts, record_id = get_checkpoint(Path(tmp) / "state.json")
            self.assertEqual(record_id, -1)
            self.assertEqual(ts.year, 1970)

    def test_checkpoint_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            ts = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
            update_checkpoint(path, ts, 42)
            loaded_ts, loaded_id = get_checkpoint(path)
            self.assertEqual(loaded_ts, ts)
            self.assertEqual(loaded_id, 42)

    def test_checkpoint_is_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            update_checkpoint(path, "2026-09-20T12:00:00+00:00", 42)
            payload = json.loads(path.read_text())
            self.assertEqual(payload["last_id"], 42)


if __name__ == "__main__":
    unittest.main()
