import unittest

from producer.producer import normalize_row


class NormalizeTests(unittest.TestCase):
    def test_normalize_row(self):
        row = {
            "ID": 1,
            "name": "Phone",
            "category": "Category A",
            "price": 100.0,
            "last_updated": "2026-09-20 12:00:00",
        }
        result = normalize_row(row)
        self.assertEqual(result["ID"], 1)
        self.assertEqual(result["price"], 100.0)
        self.assertIsInstance(result["last_updated"], int)


if __name__ == "__main__":
    unittest.main()
