import pandas as pd
import pytest

from src.transform import (
    validate_orders,
    validate_customer_references,
)


def sample_orders():
    return pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "order_date": ["2026-10-01", "2026-10-02"],
        "amount": [1500.00, 800.00],
    })


def test_valid_orders_pass():
    result = validate_orders(sample_orders())
    assert len(result) == 2


def test_duplicate_order_ids_fail():
    orders = sample_orders()
    orders.loc[1, "order_id"] = 101

    with pytest.raises(ValueError, match="duplicate"):
        validate_orders(orders)


def test_negative_amount_fails():
    orders = sample_orders()
    orders.loc[0, "amount"] = -100

    with pytest.raises(ValueError, match="negative"):
        validate_orders(orders)


def test_invalid_customer_reference_fails():
    orders = sample_orders()
    customers = pd.DataFrame({"customer_id": [1]})

    with pytest.raises(ValueError, match="nonexistent customers"):
        validate_customer_references(orders, customers)