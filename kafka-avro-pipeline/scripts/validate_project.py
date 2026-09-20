import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    "configs/settings.py",
    "producer/checkpoint.py",
    "producer/db.py",
    "producer/producer.py",
    "spark/stream.py",
    "spark/transform.py",
    "schemas/product.avsc",
    "dags/pipeline_dag.py",
]


def main() -> None:
    errors = []

    for relative in REQUIRED:
        path = ROOT / relative
        if not path.exists():
            errors.append(f"Missing required file: {relative}")

    for path in ROOT.rglob("*.py"):
        if any(part in {"__pycache__", ".venv", "venv"} for part in path.parts):
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"Python syntax error in {path}: {exc}")

    schema_path = ROOT / "schemas/product.avsc"
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        if schema.get("type") != "record" or not schema.get("fields"):
            errors.append("Avro schema must define a record with fields")
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"Invalid Avro JSON schema: {exc}")

    if (ROOT / ".env").exists():
        errors.append(".env must not be committed; use .env.example")

    if errors:
        raise SystemExit("\n".join(errors))

    print("Kafka Avro project static validation passed.")


if __name__ == "__main__":
    main()
