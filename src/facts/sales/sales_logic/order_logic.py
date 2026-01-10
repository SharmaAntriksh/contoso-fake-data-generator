import numpy as np


def build_orders(
    rng,
    n: int,
    skip_cols: bool,
    date_pool,
    date_prob,
    customers,
    product_keys,          # kept for API stability (not used here)
    _len_date_pool: int,
    _len_customers: int,
):
    """
    Generate order-level structure and expand to line-level rows.

    Returns a dict with:
      - customer_keys
      - order_dates
      - (optionally) order_ids_int, line_num, order_ids_str
    """

    if skip_cols not in (True, False):
        raise RuntimeError("skip_cols must be a boolean")

    # ------------------------------------------------------------
    # Order count heuristic
    # ------------------------------------------------------------
    avg_lines = 2.0
    order_count = max(1, int(n / avg_lines))

    # ------------------------------------------------------------
    # Order-level data
    # ------------------------------------------------------------
    od_idx = rng.choice(_len_date_pool, size=order_count, p=date_prob)
    order_dates = date_pool[od_idx]

    # Fast YYYYMMDD integer construction
    date_int = (
        order_dates.astype("datetime64[D]")
        .astype("datetime64[D]")
        .astype(str)
    )
    date_int = np.char.replace(date_int, "-", "").astype(np.int64)

    suffix_int = rng.integers(
        0,
        1_000_000_000,
        size=order_count,
        dtype=np.int64,
    )

    order_ids_int = date_int * 1_000_000_000 + suffix_int

    cust_idx = rng.integers(0, _len_customers, size=order_count)
    order_customers = customers[cust_idx].astype(np.int64, copy=False)

    # ------------------------------------------------------------
    # Lines per order
    # ------------------------------------------------------------
    lines_per_order = rng.choice(
        np.array([1, 2, 3, 4, 5], dtype=np.int8),
        size=order_count,
        p=[0.55, 0.25, 0.10, 0.06, 0.04],
    )

    expanded_len = int(lines_per_order.sum())

    order_starts = np.empty(order_count, dtype=np.int64)
    np.cumsum(lines_per_order, out=order_starts)
    order_starts -= lines_per_order

    customer_keys = np.repeat(order_customers, lines_per_order)
    order_dates_expanded = np.repeat(order_dates, lines_per_order)

    # Only constructed once; sliced later
    sales_order_num_int = np.repeat(order_ids_int, lines_per_order)

    line_num = (
        np.arange(expanded_len, dtype=np.int64)
        - np.repeat(order_starts, lines_per_order)
        + 1
    )

    # ------------------------------------------------------------
    # Pad if needed (rare but deterministic)
    # ------------------------------------------------------------
    if expanded_len < n:
        extra = n - expanded_len
        sl = slice(0, extra)

        customer_keys = np.concatenate((customer_keys, customer_keys[sl]))
        order_dates_expanded = np.concatenate(
            (order_dates_expanded, order_dates_expanded[sl])
        )
        sales_order_num_int = np.concatenate(
            (sales_order_num_int, sales_order_num_int[sl])
        )
        line_num = np.concatenate((line_num, line_num[sl]))

    # ------------------------------------------------------------
    # Trim to exactly n rows
    # ------------------------------------------------------------
    customer_keys = customer_keys[:n]
    order_dates_expanded = order_dates_expanded[:n]
    sales_order_num_int = sales_order_num_int[:n]
    line_num = line_num[:n]

    # ------------------------------------------------------------
    # Output (strict skip semantics)
    # ------------------------------------------------------------
    result = {
        "customer_keys": customer_keys,
        "order_dates": order_dates_expanded.astype("datetime64[D]", copy=False),
    }

    if not skip_cols:
        result["order_ids_int"] = sales_order_num_int
        result["line_num"] = line_num
        # String version only when needed
        result["order_ids_str"] = sales_order_num_int.astype(str)

    return result
