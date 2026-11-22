
import logging
import tempfile
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def validate_orders(df):
    required_columns = {
        "order_id", "customer_id", "order_date", "amount"
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    if df["order_id"].isna().any():
        raise ValueError("Found missing order IDs")

    if df["order_id"].duplicated().any():
        raise ValueError("Found duplicate order IDs")

    if df["customer_id"].isna().any():
        raise ValueError("Found missing customer IDs")

    if df["amount"].isna().any():
        raise ValueError("Found missing order amounts")

    if (df["amount"] < 0).any():
        raise ValueError("Found negative order amounts")

    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="raise",
    )

    return df


def validate_customer_references(orders, customers):
    valid_customer_ids = set(customers["customer_id"])

    invalid_orders = orders[
        ~orders["customer_id"].isin(valid_customer_ids)
    ]

    if not invalid_orders.empty:
        raise ValueError(
            f"Found {len(invalid_orders)} orders "
            "referencing nonexistent customers"
        )

    logger.info("Customer reference validation passed")


def load_validated_incremental_batch(
    batch_file,
    customers_file=None,
):
    batch_file = Path(batch_file)

    if not batch_file.exists():
        raise FileNotFoundError(
            f"Incremental batch not found: {batch_file}"
        )

    if customers_file is None:
        customers_file = RAW_DIR / "customers.csv"

    customers_file = Path(customers_file)

    if not customers_file.exists():
        raise FileNotFoundError(
            f"Customer snapshot not found: {customers_file}"
        )

    orders = pd.read_csv(batch_file)
    customers = pd.read_csv(customers_file)

    orders = validate_orders(orders)
    validate_customer_references(orders, customers)

    logger.info(
        "Incremental batch validation passed: %d orders",
        len(orders),
    )

    return orders


def merge_incremental_orders(
    new_orders,
    processed_dir=None,
):
    """
    Merge validated incremental orders into the consolidated
    Silver orders file without creating duplicate order IDs.
    """

    if processed_dir is None:
        processed_dir = PROCESSED_DIR

    processed_dir = Path(processed_dir)

    processed_dir.mkdir(parents=True, exist_ok=True)
    output_path = processed_dir / "orders_clean.csv"

    # First-ever Silver load
    if not output_path.exists():
        write_csv_atomically(new_orders, output_path)

        logger.info(
            "Silver file created with %d orders",
            len(new_orders),
        )

        return new_orders

    # Load existing Silver data
    existing_orders = pd.read_csv(output_path)

    # Validate existing Silver data too
    existing_orders = validate_orders(existing_orders)
    new_orders = validate_orders(new_orders)

    # Ensure both datasets have the same schema
    if set(existing_orders.columns) != set(new_orders.columns):
        raise ValueError(
            "Schema mismatch between existing and new orders"
        )

    # Align column order
    existing_orders = existing_orders[new_orders.columns]

    # Find order IDs already present in Silver
    existing_ids = set(existing_orders["order_id"])
    new_ids = set(new_orders["order_id"])

    overlapping_ids = existing_ids.intersection(new_ids)

    # Check whether overlapping records are identical
    if overlapping_ids:
        existing_overlap = (
            existing_orders[
                existing_orders["order_id"].isin(overlapping_ids)
            ]
            .sort_values("order_id")
            .reset_index(drop=True)
        )

        new_overlap = (
            new_orders[
                new_orders["order_id"].isin(overlapping_ids)
            ]
            .sort_values("order_id")
            .reset_index(drop=True)
        )

        try:
            pd.testing.assert_frame_equal(
                existing_overlap,
                new_overlap,
                check_dtype=False,
            )
        except AssertionError as exc:
            raise ValueError(
                "Conflicting data found for existing order IDs"
            ) from exc

    # Keep only genuinely new orders
    new_only = new_orders[
        ~new_orders["order_id"].isin(existing_ids)
    ]

    # Combine historical and new records
    merged_orders = pd.concat(
        [existing_orders, new_only],
        ignore_index=True,
    )

    # Maintain consistent order
    merged_orders = (
        merged_orders
        .sort_values("order_id")
        .reset_index(drop=True)
    )

    # Save consolidated Silver data
    write_csv_atomically(merged_orders, output_path)

    logger.info(
        "Silver merge completed: %d new orders added, "
        "%d total orders",
        len(new_only),
        len(merged_orders),
    )

    return merged_orders


def write_csv_atomically(df, output_path):
    """
    Write a DataFrame safely using a temporary file
    and replace the destination only after success.
    """

    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with tempfile.NamedTemporaryFile(
        dir=output_path.parent,
        prefix=f".{output_path.stem}_",
        suffix=".tmp",
        delete=False,
    ) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        df.to_csv(temp_path, index=False)
        temp_path.replace(output_path)

    finally:
        if temp_path.exists():
            temp_path.unlink()


def run_transformation():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(RAW_DIR / "orders.csv")
    customers = pd.read_csv(RAW_DIR / "customers.csv")

    orders = validate_orders(orders)
    validate_customer_references(orders, customers)

    output_path = PROCESSED_DIR / "orders_clean.csv"
    orders.to_csv(output_path, index=False)

    logger.info(
        "Transformation completed: %d orders",
        len(orders),
    )


def transform_incremental_orders(
    batch_file,
    processed_dir=None,
    customers_file=None,
):
    validated_orders = load_validated_incremental_batch(
        batch_file,
        customers_file,
    )

    merged_orders = merge_incremental_orders(
        validated_orders,
        processed_dir,
    )

    logger.info(
        "Incremental transformation completed: "
        "%d total Silver orders",
        len(merged_orders),
    )

    return merged_orders
