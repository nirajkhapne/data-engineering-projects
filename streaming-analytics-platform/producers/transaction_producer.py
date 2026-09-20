from configs.settings import PROJECT_ROOT, settings
from utils.kafka import load_json_records, publish_records


def main() -> None:
    input_path = PROJECT_ROOT / "data" / "user_transactions.json"
    records = load_json_records(input_path)
    publish_records(settings.transaction_topic, records)


if __name__ == "__main__":
    main()
