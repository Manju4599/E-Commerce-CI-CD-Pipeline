import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

WATERMARK_FILE = PROJECT_ROOT / "data" / "state" / "watermarks.json"


def load_watermark(table_name: str) -> int:
    with open(WATERMARK_FILE, "r", encoding="utf-8") as file:
        watermarks = json.load(file)

    return watermarks.get(table_name, 0)


def save_watermark(table_name: str, last_id: int) -> None:
    with open(WATERMARK_FILE, "r", encoding="utf-8") as file:
        watermarks = json.load(file)

    watermarks[table_name] = last_id

    with open(WATERMARK_FILE, "w", encoding="utf-8") as file:
        json.dump(watermarks, file, indent=4)