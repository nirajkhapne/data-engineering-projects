#!/usr/bin/env python3
"""Dependency-free contract simulation using the repository's synthetic fixtures."""

import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / "src"))
from gizmobox.contracts import parse_json_lines
DATA = ROOT / "data/sample"


def read_json_lines(folder):
    valid, rejected = [], []
    for path in sorted(folder.glob("*.json")):
        parsed, invalid = parse_json_lines(path.read_text(encoding="utf-8"))
        valid.extend((path.name, record) for _, record in parsed)
        rejected.extend((path.name, item) for item in invalid)
    return valid, rejected


def read_csv_rows(folder, names, header=False):
    rows = []
    for path in sorted(folder.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            if header:
                next(reader, None)
            for row in reader:
                rows.append(dict(zip(names, row)))
    return rows


def run():
    customer_rows, _ = read_json_lines(DATA / "gizmobox/customers")
    incoming_customers, incoming_rejected = read_json_lines(DATA / "incoming/customers")
    customer_rows.extend(incoming_customers)
    customer_latest = {}
    for source_file, row in customer_rows:
        key = int(row["customer_id"])
        current = customer_latest.get(key)
        if current is None or row["created_timestamp"] > current[1]["created_timestamp"]:
            customer_latest[key] = (source_file, row)

    order_rows, order_rejected = read_json_lines(DATA / "gizmobox/orders")
    payments = read_csv_rows(DATA / "gizmobox/payments", ["payment_id", "order_id", "payment_timestamp", "payment_status", "payment_method"])
    refunds = read_csv_rows(DATA / "gizmobox/refunds", ["refund_id", "payment_id", "refund_timestamp", "refund_amount", "refund_reason"], header=True)
    orders_by_id = {int(row["order_id"]): row for _, row in order_rows}
    payments_by_id = {int(row["payment_id"]): row for row in payments}

    orphan_payments = [row for row in payments if int(row["order_id"]) not in orders_by_id]
    refunds_without_payment = [row for row in refunds if int(row["payment_id"]) not in payments_by_id]
    monthly = defaultdict(lambda: {"orders": 0, "completed_orders": 0, "gross_completed": 0.0, "refund_amount": 0.0, "net_completed": 0.0})
    refunds_by_order = defaultdict(float)
    for refund in refunds:
        payment = payments_by_id.get(int(refund["payment_id"]))
        if payment:
            refunds_by_order[int(payment["order_id"])] += float(refund["refund_amount"])
    for order_id, row in orders_by_id.items():
        month = row["order_date"][:7]
        record = monthly[month]
        record["orders"] += 1
        refund_amount = refunds_by_order[order_id]
        record["refund_amount"] += refund_amount
        if row["order_status"].strip().title() == "Completed":
            amount = float(row["total_amount"])
            record["completed_orders"] += 1
            record["gross_completed"] += amount
            record["net_completed"] += amount - refund_amount

    print("GizmoBox synthetic contract simulation")
    print(f"Customer source rows: {len(customer_rows)}; current customers: {len(customer_latest)}")
    print(f"Valid orders: {len(order_rows)}; quarantined malformed order lines: {len(order_rejected)}")
    print(f"Payments: {len(payments)}; orphan payment/order links: {len(orphan_payments)}")
    print(f"Refunds: {len(refunds)}; refunds without payment: {len(refunds_without_payment)}")
    print("Monthly summary:")
    for month, values in sorted(monthly.items()):
        print(f"  {month}: {values}")
    if len(order_rejected) != 1 or len(customer_latest) != 4 or len(orders_by_id) != 3 or len(orphan_payments) != 1 or refunds_without_payment:
        raise SystemExit("Simulation failed: fixture contracts differ from the documented expectations.")
    print("Simulation passed. The single orphan payment is expected because its order is the malformed sample record.")


if __name__ == "__main__":
    run()
