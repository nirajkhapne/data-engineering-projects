import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import producer.producer as producer_module


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.result = []

    def execute(self, _query, params):
        last_updated, _, last_id, batch_size = params
        eligible = [
            row
            for row in self.rows
            if row["last_updated"] > last_updated
            or (row["last_updated"] == last_updated and row["ID"] > last_id)
        ]
        eligible.sort(key=lambda row: (row["last_updated"], row["ID"]))
        self.result = eligible[:batch_size]

    def fetchall(self):
        return list(self.result)

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self, dictionary=True):
        return FakeCursor(self.rows)

    def close(self):
        pass


class FakeProducer:
    def __init__(self):
        self.records = []
        self.commits = 0
        self.aborts = 0

    def init_transactions(self):
        pass

    def begin_transaction(self):
        pass

    def produce(self, **kwargs):
        self.records.append(kwargs["value"])

    def poll(self, _timeout):
        pass

    def commit_transaction(self):
        self.commits += 1

    def abort_transaction(self):
        self.aborts += 1

    def flush(self, _timeout):
        pass


class IncrementalSimulationTests(unittest.TestCase):
    def test_checkpoint_does_not_advance_when_transaction_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Path(tmp) / "producer_state.json"
            timestamp = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)
            rows = [{
                "ID": 1, "name": "A", "category": "Category A", "price": 10,
                "last_updated": timestamp,
            }]

            class FailingProducer(FakeProducer):
                def commit_transaction(self):
                    self.commits += 1
                    raise RuntimeError("simulated Kafka commit failure")

            fake_producer = FailingProducer()
            fake_settings = SimpleNamespace(
                checkpoint_path=checkpoint,
                batch_size=2,
                product_topic="product_updates",
                ensure_local_dirs=lambda: checkpoint.parent.mkdir(parents=True, exist_ok=True),
            )

            with patch.object(producer_module, "settings", fake_settings), \
                 patch.object(producer_module, "create_connection", return_value=FakeConnection(rows)), \
                 patch.object(producer_module, "build_producer", return_value=fake_producer):
                with self.assertRaises(RuntimeError):
                    producer_module.run()

            self.assertFalse(checkpoint.exists())
            self.assertEqual(fake_producer.aborts, 1)

    def test_same_timestamp_records_are_not_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Path(tmp) / "producer_state.json"
            timestamp = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)
            rows = [
                {"ID": 1, "name": "A", "category": "Category A", "price": 10, "last_updated": timestamp},
                {"ID": 2, "name": "B", "category": "Category B", "price": 20, "last_updated": timestamp},
                {"ID": 3, "name": "C", "category": "Category C", "price": 30, "last_updated": timestamp},
            ]

            fake_producer = FakeProducer()
            fake_settings = SimpleNamespace(
                checkpoint_path=checkpoint,
                batch_size=2,
                product_topic="product_updates",
                ensure_local_dirs=lambda: checkpoint.parent.mkdir(parents=True, exist_ok=True),
            )

            with patch.object(producer_module, "settings", fake_settings), \
                 patch.object(producer_module, "create_connection", return_value=FakeConnection(rows)), \
                 patch.object(producer_module, "build_producer", return_value=fake_producer):
                count = producer_module.run()

            self.assertEqual(count, 3)
            self.assertEqual([row["ID"] for row in fake_producer.records], [1, 2, 3])
            self.assertEqual(fake_producer.commits, 2)
            self.assertEqual(fake_producer.aborts, 0)

            saved_ts, saved_id = producer_module.get_checkpoint(checkpoint)
            self.assertEqual(saved_ts, timestamp)
            self.assertEqual(saved_id, 3)


if __name__ == "__main__":
    unittest.main()
