import json

from src.watermark import load_watermark, save_watermark


def test_load_watermark(tmp_path, monkeypatch):
    watermark_file = tmp_path / "watermarks.json"

    watermark_file.write_text(
        json.dumps({"orders": 103}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.watermark.WATERMARK_FILE",
        watermark_file,
    )

    assert load_watermark("orders") == 103


def test_save_watermark(tmp_path, monkeypatch):
    watermark_file = tmp_path / "watermarks.json"

    watermark_file.write_text(
        json.dumps({"orders": 103}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.watermark.WATERMARK_FILE",
        watermark_file,
    )

    save_watermark("orders", 106)

    assert load_watermark("orders") == 106