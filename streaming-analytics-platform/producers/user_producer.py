import json

from configs.settings import PROJECT_ROOT, settings
from utils.kafka import publish_records


def main() -> None:
    input_path = PROJECT_ROOT / "data" / "user_data.json"
    records = []
    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {input_path}:{line_number}") from exc

    publish_records(settings.user_topic, records)


if __name__ == "__main__":
    main()
