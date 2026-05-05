def transform(record):
    record["category"] = record["category"].lower()

    if record["category"] == "category a":
        record["price"] = round(record["price"] * 0.5, 2)

    return record
