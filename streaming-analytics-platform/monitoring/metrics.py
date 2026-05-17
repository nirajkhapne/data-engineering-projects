from prometheus_client import Counter

processed_records = Counter(
    'processed_records_total',
    'Total Processed records'
)
