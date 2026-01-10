import numpy as np


def build_orders(
    rng, n, skip_cols,
    date_pool, date_prob,
    customers, product_keys,
    _len_date_pool, _len_customers
):
    avg_lines = 2.0
    order_count = max(1, int(n / avg_lines))

    # ------------------------------------------------------------
    # Order-level data
    # ------------------------------------------------------------

    od_idx = rng.choice(_len_date_pool, size=order_count, p=date_prob)
    order_dates = date_pool[od_idx]

    date_str = np.datetime_as_string(order_dates, unit="D")
    date_int = np.char.replace(date_str, "-", "").astype(np.int64)

    suffix_int = rng.integers(
        0, 1_000_000_000,
        size=order_count,
        dtype=np.int64
    )

    order_ids_int = date_int * 1_000_000_000 + suffix_int

    cust_idx = rng.integers(0, _len_customers, size=order_count)
    order_customers = customers[cust_idx].astype(np.int64, copy=False)

    # ------------------------------------------------------------
    # Lines per order
    # ------------------------------------------------------------

    lines_per_order = rng.choice(
        [1, 2, 3, 4, 5],
        size=order_count,
        p=[0.55, 0.25, 0.10, 0.06, 0.04],
    )

    expanded_len = int(lines_per_order.sum())
    order_starts = np.cumsum(lines_per_order) - lines_per_order

    customer_keys = np.repeat(order_customers, lines_per_order)
    order_dates_expanded = np.repeat(order_dates, lines_per_order)

    # These are only needed if order cols are emitted
    sales_order_num_int = np.repeat(order_ids_int, lines_per_order)
    line_num = (
        np.arange(expanded_len) -
        np.repeat(order_starts, lines_per_order) +
        1
    ).astype(np.int64)

    # ------------------------------------------------------------
    # Pad if needed
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
    # Output (TRUE skip semantics)
    # ------------------------------------------------------------

    result = {
        "customer_keys": customer_keys,
        "order_dates": order_dates_expanded.astype("datetime64[D]", copy=False),
    }

    if not skip_cols:
        result["order_ids_int"] = sales_order_num_int
        result["line_num"] = line_num
        result["order_ids_str"] = sales_order_num_int.astype(str)

    return result
