import json
import os

def write_json(record, instance_id):
    os.makedirs("data/output", exist_ok=True)

    with open(f"data/output/consumer_{instance_id}.json", "a") as f:
        f.write(json.dumps(record) + "\n")
