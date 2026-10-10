import csv
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data/sample"
sys.path.insert(0, str(ROOT / "src"))
from gizmobox.contracts import parse_json_lines


class SampleDataTests(unittest.TestCase):
    def test_one_malformed_order_line_is_present_for_quarantine(self):
        valid_count = rejected_count = 0
        for path in (DATA / "gizmobox/orders").glob("*.json"):
            valid, rejected = parse_json_lines(path.read_text(encoding="utf-8"))
            valid_count += len(valid)
            rejected_count += len(rejected)
        self.assertEqual(valid_count, 3)
        self.assertEqual(rejected_count, 1)

    def test_incoming_customer_is_valid_json(self):
        path = DATA / "incoming/customers/customer_delta_2024_12.json"
        row = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(row["customer_id"], 1004)

    def test_payment_files_are_headerless_five_column_csv(self):
        files = list((DATA / "gizmobox/payments").glob("*.csv"))
        self.assertGreaterEqual(len(files), 1)
        for path in files:
            with path.open(newline="", encoding="utf-8") as handle:
                for row in csv.reader(handle):
                    self.assertEqual(len(row), 5)
                    self.assertTrue(row[0].isdigit())

    def test_membership_fixture_uses_customer_and_month_path(self):
        files = list((DATA / "gizmobox/memberships").glob("*/*.png"))
        self.assertEqual(len(files), 4)
        self.assertTrue(all(path.parent.name in {"2024-10", "2024-11"} for path in files))


if __name__ == "__main__":
    unittest.main()
