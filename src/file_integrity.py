
from pathlib import Path
import hashlib


def calculate_sha256(file_path: Path) -> str:
    """Return the SHA-256 fingerprint of a file."""

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while True:
            chunk = file.read(8192)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    csv_file = project_root / "data" / "raw" / "orders.csv"

    if not csv_file.is_file():
        raise FileNotFoundError(f"File not found: {csv_file}")

    print("File:", csv_file.name)
    print("SHA-256:", calculate_sha256(csv_file))
