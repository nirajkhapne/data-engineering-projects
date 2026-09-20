from prometheus_client import Counter

processed_records = Counter(
    "processed_records_total",
    "Total number of records written to the MongoDB serving layer",
)
