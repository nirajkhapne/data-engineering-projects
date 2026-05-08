from prometheus_client import Counter

messages_processed = Counter(
    'messages_processed_total',
    'Total processed Kafka messages'
)
