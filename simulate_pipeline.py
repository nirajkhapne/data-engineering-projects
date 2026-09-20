import csv
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/mock_product.csv"


def read_rows():
    with SOURCE.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def normalize(rows):
    normalized = []
    for row in rows:
        normalized.append(
            {
                "ID": int(row["ID"]),
                "name": row["name"],
                "category": row["category"].lower(),
                "price": float(row["price"]),
                "last_updated": int(
                    datetime.fromisoformat(row["last_updated"])
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                    * 1000
                ),
            }
        )
    return normalized


def apply_business_rule(rows):
    for row in rows:
        if row["category"] == "category a":
            row["price"] *= 0.5
    return rows


def main():
    rows = apply_business_rule(normalize(read_rows()))
    print(f"Input rows: {len(rows)}")
    print("Transformed rows:")
    for row in rows:
        print(
            f"  ID={row['ID']} | category={row['category']} | "
            f"price={row['price']:.2f} | last_updated={row['last_updated']}"
        )

    latest = max(rows, key=lambda row: (row["last_updated"], row["ID"]))
    print(
        "Simulated checkpoint: "
        f"(last_updated={latest['last_updated']}, last_id={latest['ID']})"
    )


if __name__ == "__main__":
    main()
