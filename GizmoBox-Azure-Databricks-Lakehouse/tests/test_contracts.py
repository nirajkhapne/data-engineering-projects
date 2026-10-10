import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from gizmobox.contracts import membership_identity, parse_json_lines
from gizmobox.config import PipelineConfig


class ContractTests(unittest.TestCase):
    def test_membership_identity_from_path(self):
        self.assertEqual(membership_identity("/landing/memberships/2024-10/1001.png"), (1001, "2024-10"))

    def test_invalid_membership_path_is_rejected(self):
        self.assertIsNone(membership_identity("/landing/memberships/unknown/card.png"))

    def test_json_lines_preserve_malformed_input(self):
        valid, rejected = parse_json_lines('{"id": 1}\n{"id":\n')
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0][1]["id"], 1)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["line_number"], 2)
        self.assertIn("raw_record", rejected[0])

    def test_config_builds_three_part_table_name(self):
        config = PipelineConfig(catalog="gizmobox", landing_root="abfss://container@storage.dfs.core.windows.net/landing")
        self.assertEqual(config.table("silver", "customers"), "`gizmobox`.`silver`.`customers`")

    def test_source_path_joins_cleanly(self):
        config = PipelineConfig(catalog="gizmobox", landing_root="abfss://container@storage.dfs.core.windows.net/landing/")
        self.assertEqual(config.source_path("backfill", "gizmobox"), "abfss://container@storage.dfs.core.windows.net/landing/backfill/gizmobox")


if __name__ == "__main__":
    unittest.main()
