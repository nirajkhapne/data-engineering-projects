import json
import os

FILE = "state/processed_ids.json"

def load_ids():
    if not os.path.exists(FILE):
        return set()
    with open(FILE) as f:
        return set(json.load(f))

def save_ids(ids):
    with open(FILE, "w") as f:
        json.dump(list(ids), f)

def is_processed(record_id, ids):
    return record_id in ids

def mark_processed(record_id, ids):
    ids.add(record_id)
    save_ids(ids)
