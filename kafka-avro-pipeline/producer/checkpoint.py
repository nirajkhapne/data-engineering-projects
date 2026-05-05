import json

FILE = "producer/state.json"

def get_last_ts():
    try:
        with open(FILE) as f:
            return json.load(f)["last_ts"]
    except:
        return "1970-01-01 00:00:00"

def update_last_ts(ts):
    with open(FILE, "w") as f:
        json.dump({"last_ts": str(ts)}, f)
