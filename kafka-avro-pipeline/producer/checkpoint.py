import json
import os

FILE = "state/producer_state.json"

def get_last_ts():
    if not os.path.exists(FILE):
        return "1970-01-01 00:00:00"

    with open(FILE) as f:
        return json.load(f).get("last_ts")

def update_last_ts(ts):
    os.makedirs("state", exist_ok=True)

    with open(FILE, "w") as f:
        json.dump({"last_ts": ts}, f)
