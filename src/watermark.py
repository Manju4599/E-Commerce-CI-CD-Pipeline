


import json
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

WATERMARK_FILE = PROJECT_ROOT / "data" / "state" / "watermarks.json"



def load_watermark(
    table_name: str,
    watermark_file: Path | None = None,
) -> int:
    if watermark_file is None:
        watermark_file = WATERMARK_FILE

    with open(watermark_file, "r", encoding="utf-8") as file:
        watermarks = json.load(file)

    return watermarks.get(table_name, 0)


def save_watermark(
    table_name: str,
    last_id: int,
    watermark_file: Path | None = None,
) -> None:
    if watermark_file is None:
        watermark_file = WATERMARK_FILE

    if not isinstance(last_id, int) or isinstance(last_id, bool):
        raise TypeError("Watermark must be an integer")

    if last_id < 0:
        raise ValueError("Watermark cannot be negative")

    watermark_file.parent.mkdir(parents=True, exist_ok=True)

    if watermark_file.exists():
        with open(watermark_file, "r", encoding="utf-8") as file:
            watermarks = json.load(file)
    else:
        watermarks = {}

    current_watermark = watermarks.get(table_name, 0)

    if last_id < current_watermark:
        raise ValueError(
            f"Watermark cannot move backwards: "
            f"{current_watermark} -> {last_id}"
        )

    watermarks[table_name] = last_id

    temp_file = watermark_file.with_suffix(".tmp")

    try:
        with open(temp_file, "w", encoding="utf-8") as file:
            json.dump(watermarks, file, indent=4)
            file.flush()
            os.fsync(file.fileno())

        temp_file.replace(watermark_file)

    finally:
        if temp_file.exists():
            temp_file.unlink()
