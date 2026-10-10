"""Dependency-free helpers for validating source naming and records."""

import json
import re
from pathlib import PurePosixPath


def membership_identity(path: str):
    """Return (customer_id, month) only when both can be read from the path."""
    parts = PurePosixPath(path.replace("\\", "/")).parts
    filename = parts[-1] if parts else ""
    month = parts[-2] if len(parts) >= 2 else ""
    match = re.fullmatch(r"(\d+)\.png", filename, flags=re.IGNORECASE)
    if not match or not re.fullmatch(r"\d{4}-\d{2}", month):
        return None
    return int(match.group(1)), month


def parse_json_lines(text: str):
    """Split JSON Lines input into parsed records and rejected raw lines."""
    parsed, rejected = [], []
    for line_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            parsed.append((line_number, json.loads(raw)))
        except json.JSONDecodeError as exc:
            rejected.append({"line_number": line_number, "raw_record": raw, "reason": str(exc)})
    return parsed, rejected
