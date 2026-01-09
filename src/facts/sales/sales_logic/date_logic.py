import numpy as np


def compute_dates(rng, n, product_keys, order_ids_int, order_dates):
    """
    Compute due dates, delivery dates, delivery status, and order delay flag.

    Supports:
    - order_ids_int present  → order-level coherent behavior
    - order_ids_int is None → row-level fallback (skip_order_cols=True)
    """

    # ------------------------------------------------------------
    # MODE SELECTION
    # ------------------------------------------------------------
    has_orders = order_ids_int is not None

    if has_orders:
        hash_vals = order_ids_int.astype(np.int64, copy=False)
    else:
        # Deterministic-ish surrogate per row
        hash_vals = (
            product_keys.astype(np.int64, copy=False)
            + rng.integers(0, 1_000_000, size=n, dtype=np.int64)
        )

    # ------------------------------------------------------------
    # Due dates
    # ------------------------------------------------------------
    due_offset = (hash_vals % 5) + 3
    due_date = order_dates + due_offset.astype("timedelta64[D]")

    # ------------------------------------------------------------
    # Seeds (vectorized, cheap)
    # ------------------------------------------------------------
    order_seed = hash_vals % 100
    product_seed = (hash_vals + product_keys) % 100
    line_seed = (product_keys + (hash_vals % 100)) % 100

    # ------------------------------------------------------------
    # Base delivery offset
    # ------------------------------------------------------------
    delivery_offset = np.zeros(n, dtype=np.int64)

    # Condition C
    mask_c = (order_seed >= 60) & (order_seed < 85) & (product_seed >= 60)
    delivery_offset[mask_c] = (line_seed[mask_c] % 4) + 1

    # Condition D
    mask_d = order_seed >= 85
    delivery_offset[mask_d] = (product_seed[mask_d] % 5) + 2

    # ------------------------------------------------------------
    # Early deliveries (10%)
    # ------------------------------------------------------------
    early_mask = rng.random(n) < 0.10
    if early_mask.any():
        early_days = rng.integers(1, 3, size=n)
        delivery_offset[early_mask] = -early_days[early_mask]

    delivery_date = due_date + delivery_offset.astype("timedelta64[D]")

    # ------------------------------------------------------------
    # Delivery status
    # ------------------------------------------------------------
    delivery_status = np.full(n, "On Time", dtype="U15")
    delivery_status[delivery_date < due_date] = "Early Delivery"
    delivery_status[delivery_date > due_date] = "Delayed"

    # ------------------------------------------------------------
    # Order delayed flag
    # ------------------------------------------------------------
    if has_orders:
        delayed_line = (delivery_status == "Delayed")

        # Map rows → order index
        _, inv_idx = np.unique(order_ids_int, return_inverse=True)

        # Any delayed line → order delayed
        delayed_any = np.bincount(inv_idx, weights=delayed_line).astype(bool)
        is_order_delayed = delayed_any[inv_idx].astype(np.int8)
    else:
        # Row-level fallback
        is_order_delayed = (delivery_status == "Delayed").astype(np.int8)

    return {
        "due_date": due_date,
        "delivery_date": delivery_date,
        "delivery_status": delivery_status,
        "is_order_delayed": is_order_delayed,
    }
