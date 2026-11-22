import json
from src.watermark import load_watermark, save_watermark
import pytest
from unittest.mock import patch

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
    




def test_watermark_cannot_move_backwards(tmp_path, monkeypatch):
    watermark_file = tmp_path / "watermarks.json"
    watermark_file.write_text(
        json.dumps({"orders": 109}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.watermark.WATERMARK_FILE",
        watermark_file,
    )

    with pytest.raises(ValueError, match="cannot move backwards"):
        save_watermark("orders", 108)

    assert load_watermark("orders") == 109


def test_watermark_rejects_invalid_values(tmp_path, monkeypatch):
    watermark_file = tmp_path / "watermarks.json"
    watermark_file.write_text(
        json.dumps({"orders": 109}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.watermark.WATERMARK_FILE",
        watermark_file,
    )

    with pytest.raises(TypeError):
        save_watermark("orders", "110")

    with pytest.raises(TypeError):
        save_watermark("orders", True)

    with pytest.raises(ValueError):
        save_watermark("orders", -1)


def test_failed_atomic_write_preserves_existing_watermark(
    tmp_path, monkeypatch
):
    watermark_file = tmp_path / "watermarks.json"
    watermark_file.write_text(
        json.dumps({"orders": 109}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.watermark.WATERMARK_FILE",
        watermark_file,
    )

    with patch(
        "src.watermark.Path.replace",
        side_effect=OSError("Simulated replacement failure"),
    ):
        with pytest.raises(
            OSError,
            match="Simulated replacement failure",
        ):
            save_watermark("orders", 110)

    assert load_watermark("orders") == 109
    assert not watermark_file.with_suffix(".tmp").exists()
