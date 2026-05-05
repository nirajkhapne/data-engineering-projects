import json
import os

def write_json(record, instance_id):
    path = f"data/output/consumer_{instance_id}.json"

    os.makedirs("data/output", exist_ok=True)

    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")
