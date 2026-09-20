import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_TIMESTAMP = "1970-01-01T00:00:00+00:00"


def _normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_checkpoint(path: Path) -> tuple[datetime, int]:
    """Return the last processed (timestamp, ID) pair."""
    if not path.exists():
        return _normalize_timestamp(DEFAULT_TIMESTAMP), -1

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return _normalize_timestamp(payload.get("last_updated", DEFAULT_TIMESTAMP)), int(
            payload.get("last_id", -1)
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid producer checkpoint: {path}") from exc


def update_checkpoint(path: Path, last_updated: Any, last_id: int) -> None:
    """Atomically persist the latest successfully committed source position."""
    timestamp = _normalize_timestamp(last_updated)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "last_updated": timestamp.isoformat(),
        "last_id": int(last_id),
    }
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)
