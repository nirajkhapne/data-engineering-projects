"""Run dependency-free checks before committing the project."""

import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON_DIRS = [ROOT / "api", ROOT / "configs", ROOT / "mongodb", ROOT / "monitoring", ROOT / "producers", ROOT / "schemas", ROOT / "streaming", ROOT / "utils"]


def validate_python() -> list[str]:
    errors = []
    for directory in PYTHON_DIRS:
        for path in directory.glob("*.py"):
            try:
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as exc:
                errors.append(f"{path}: {exc}")
    return errors


def validate_json() -> list[str]:
    errors = []
    for path in (ROOT / "data").glob("*.json"):
        try:
            if path.name == "user_data.json":
                for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                    if line.strip():
                        json.loads(line)
            else:
                json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"{path}: {exc}")
    return errors


def main() -> int:
    errors = validate_python() + validate_json()
    if errors:
        print("Validation failed:")
        print("\n".join(f"- {error}" for error in errors))
        return 1

    print("Python syntax and JSON validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
