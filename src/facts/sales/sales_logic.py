# sales_logic.py
import numpy as np
import pandas as pd
import pyarrow as pa

# Globals assigned by worker initializer
_G_skip_order_cols = None
_G_product_np = None
_G_customers = None
_G_date_pool = None
_G_date_prob = None
_G_store_keys = None
_G_promo_keys_all = None
_G_promo_pct_all = None
_G_promo_start_all = None
_G_promo_end_all = None
_G_store_to_geo_arr = None
_G_geo_to_currency_arr = None
_G_store_to_geo = None
_G_geo_to_currency = None


def bind_globals(gdict):
    """Worker initializer will inject globals here."""
    globals().update(gdict)


def _build_chunk_table(n, seed, no_discount_key=1):
    """
    Optimized chunk builder — vectorized and memory-safe.
    """
    rng = np.random.default_rng(seed)
    skip_cols = _G_skip_order_cols

    product_np = _G_product_np
    customers = _G_customers
    date_pool = _G_date_pool
    date_prob = _G_date_prob
    store_keys = _G_store_keys
    promo_keys_all = _G_promo_keys_all
    promo_pct_all = _G_promo_pct_all
    promo_start_all = _G_promo_start_all
    promo_end_all = _G_promo_end_all

    st2g_arr = _G_store_to_geo_arr
    g2c_arr = _G_geo_to_currency_arr
    store_to_geo = _G_store_to_geo
    geo_to_currency = _G_geo_to_currency

    # ---------------------------------------------------------
    # PRODUCTS
    # ---------------------------------------------------------
    prod_idx = rng.integers(0, len(product_np), size=n)
    prods = product_np[prod_idx]
    product_keys = prods[:, 0].astype(np.int64)
    unit_price = prods[:, 1].astype(np.float64)
    unit_cost = prods[:, 2].astype(np.float64)

    # ---------------------------------------------------------
    # STORES -> GEO -> CURRENCY mapping
    # ---------------------------------------------------------
    store_key_arr = store_keys[rng.integers(0, len(store_keys), size=n)].astype(np.int64)

    try:
        if st2g_arr is not None and g2c_arr is not None:
            geo_arr = st2g_arr[store_key_arr]
            currency_arr = g2c_arr[geo_arr]
        else:
            geo_arr = np.array([store_to_geo[s] for s in store_key_arr])
            currency_arr = np.array([geo_to_currency[g] for g in geo_arr], dtype=np.int64)
    except Exception:
        geo_arr = np.array([store_to_geo[s] for s in store_key_arr])
        currency_arr = np.array([geo_to_currency[g] for g in geo_arr], dtype=np.int64)

    # ---------------------------------------------------------
    # QUANTITY
    # ---------------------------------------------------------
    qty = np.clip(rng.poisson(3, n) + 1, 1, 10).astype(np.int64)

    # --- ORDER GROUPING (fast vectorized)
    avg_lines = 2.0
    order_count = max(1, int(n / avg_lines))

    suffix = np.char.zfill(rng.integers(0, 999999, order_count).astype(str), 6)
    od_idx = rng.choice(len(date_pool), size=order_count, p=date_prob)
    order_dates = date_pool[od_idx]

    # string ids for order number
    order_dates_str = np.array([str(d.astype("datetime64[D]"))[:10].replace("-", "") for d in order_dates])
    order_ids_str = np.char.add(order_dates_str, suffix)
    order_ids_int = order_ids_str.astype(np.int64)

    cust_idx = rng.integers(0, len(customers), order_count)
    order_customers = customers[cust_idx].astype(np.int64)

    lines_per_order = rng.choice([1, 2, 3, 4, 5], order_count, p=[0.55, 0.25, 0.10, 0.06, 0.04])

    # build expanded arrays (may overshoot)
    expanded_len = lines_per_order.sum()
    order_idx = np.repeat(np.arange(order_count), lines_per_order)
    # starts = cumulative start indices per order
    starts = np.repeat(np.cumsum(lines_per_order) - lines_per_order, lines_per_order)
    # line numbers within each order: 1..k
    line_num = (np.arange(expanded_len) - starts + 1).astype(np.int64)

    sales_order_num = np.repeat(order_ids_str, lines_per_order)
    sales_order_num_int = np.repeat(order_ids_int, lines_per_order)
    customer_keys = np.repeat(order_customers, lines_per_order)
    order_dates_expanded = np.repeat(order_dates, lines_per_order)

    # pad if undershoot
    current_len = len(sales_order_num)
    if current_len < n:
        extra = n - current_len
        extra_suffix = np.char.zfill(rng.integers(0, 999999, extra).astype(str), 6)
        extra_dates = date_pool[rng.choice(len(date_pool), size=extra, p=date_prob)]

        extra_dates_str = np.array([str(d.astype("datetime64[D]"))[:10].replace("-", "") for d in extra_dates])
        extra_ids_str = np.char.add(extra_dates_str, extra_suffix)
        extra_ids_int = extra_ids_str.astype(np.int64)

        sales_order_num = np.concatenate([sales_order_num, extra_ids_str])
        sales_order_num_int = np.concatenate([sales_order_num_int, extra_ids_int])
        # pad line numbers with 1's for new single-line orders
        line_num = np.concatenate([line_num, np.ones(extra, dtype=np.int64)])
        customer_keys = np.concatenate([customer_keys,
                                        customers[rng.integers(0, len(customers), extra)]])
        order_dates_expanded = np.concatenate([order_dates_expanded, extra_dates])

    # final trim to exactly n
    sales_order_num = sales_order_num[:n]
    sales_order_num_int = sales_order_num_int[:n]
    line_num = line_num[:n].astype(np.int64)
    customer_keys = customer_keys[:n].astype(np.int64)
    order_dates_expanded = order_dates_expanded[:n]

    # cache date array used later
    od_np = order_dates_expanded.astype("datetime64[D]")

    # ---------------------------------------------------------
    # DELIVERY LOGIC (depends on order_id) - masked assignments
    # ---------------------------------------------------------
    hash_vals = sales_order_num_int

    due_offset = (hash_vals % 5).astype(np.int64) + 3
    due_date_np = od_np + due_offset.astype("timedelta64[D]")

    line_seed = (product_keys + (hash_vals % 100)) % 100
    product_seed = (hash_vals + product_keys) % 100
    order_seed = (hash_vals % 100).astype(np.int64)

    # masked assignment instead of np.select
    base_offset = np.zeros(n, dtype=np.int64)

    # cond_a and cond_b set zero (no-op)
    mask_c = (order_seed >= 60) & (order_seed < 85) & (product_seed >= 60)
    if mask_c.any():
        base_offset[mask_c] = (line_seed[mask_c] % 4) + 1

    mask_d = order_seed >= 85
    if mask_d.any():
        base_offset[mask_d] = (product_seed[mask_d] % 5) + 2

    early_mask = rng.random(n) < 0.10
    early_days = rng.integers(1, 3, n)
    delivery_offset = base_offset.copy()
    delivery_offset[early_mask] = -early_days[early_mask]

    delivery_date_np = due_date_np + delivery_offset.astype("timedelta64[D]")

    delivery_status = np.where(
        delivery_date_np < due_date_np, "Early Delivery",
        np.where(delivery_date_np > due_date_np, "Delayed", "On Time")
    )

    # ---------------------------------------------------------
    # PROMOTIONS (memory-safe)
    # ---------------------------------------------------------
    promo_keys = np.full(n, no_discount_key, dtype=np.int64)
    promo_pct = np.zeros(n, dtype=np.float64)

    if promo_keys_all is not None and promo_keys_all.size > 0:
        for pk, pct, start, end in zip(promo_keys_all, promo_pct_all, promo_start_all, promo_end_all):
            mask = (od_np >= start) & (od_np <= end)
            if mask.any():
                promo_keys[mask] = pk
                promo_pct[mask] = pct

    # ---------------------------------------------------------
    # DISCOUNT LOGIC
    # ---------------------------------------------------------
    promo_disc = unit_price * (promo_pct / 100.0)

    rnd_pct = rng.choice([0, 5, 10, 15, 20], n, p=[0.85, 0.06, 0.04, 0.03, 0.02])
    rnd_disc = unit_price * (rnd_pct / 100.0)

    discount_amt = np.maximum(promo_disc, rnd_disc)
    discount_amt *= rng.choice([0.90, 0.95, 1.00, 1.05, 1.10], n)
    discount_amt = np.round(discount_amt * 4) / 4
    discount_amt = np.minimum(discount_amt, unit_price - 0.01)

    # ---------------------------------------------------------
    # IS ORDER DELAYED
    # ---------------------------------------------------------
    is_delayed_line = (delivery_status == "Delayed").astype(np.int64)

    unique_ids, inverse_idx = np.unique(sales_order_num, return_inverse=True)
    counts = np.bincount(inverse_idx, weights=is_delayed_line, minlength=len(unique_ids))
    delayed_any = (counts > 0).astype(np.int8)
    is_order_delayed = delayed_any[inverse_idx].astype(np.int8)

    # ---------------------------------------------------------
    # FINAL PRICES (vectorized factor)
    # ---------------------------------------------------------
    factor = rng.uniform(0.43, 0.61, size=n)
    final_unit_price = np.round(unit_price * factor, 2)
    final_unit_cost = np.round(unit_cost * factor, 2)
    final_discount_amt = np.round(discount_amt * factor, 2)
    final_net_price = np.round(final_unit_price - final_discount_amt, 2)
    final_net_price = np.clip(final_net_price, 0.01, None)

    # ---------------------------------------------------------
    # BUILD PYARROW OR PANDAS (minimize extra casts)
    # ---------------------------------------------------------
    if pa is not None:
        pa_cols = {
            "OrderDate": pa.array(od_np),
            "DueDate": pa.array(due_date_np.astype("datetime64[D]")),
            "DeliveryDate": pa.array(delivery_date_np.astype("datetime64[D]")),
            "StoreKey": pa.array(store_key_arr, type=pa.int64()),
            "ProductKey": pa.array(product_keys, type=pa.int64()),
            "PromotionKey": pa.array(promo_keys, type=pa.int64()),
            "CurrencyKey": pa.array(currency_arr, type=pa.int64()),
            "CustomerKey": pa.array(customer_keys, type=pa.int64()),
            "Quantity": pa.array(qty, type=pa.int64()),
            "NetPrice": pa.array(final_net_price, type=pa.float64()),
            "UnitCost": pa.array(final_unit_cost, type=pa.float64()),
            "UnitPrice": pa.array(final_unit_price, type=pa.float64()),
            "DiscountAmount": pa.array(final_discount_amt, type=pa.float64()),
            "DeliveryStatus": pa.array(delivery_status.tolist(), type=pa.string()),
            "IsOrderDelayed": pa.array(is_order_delayed.astype(np.int8), type=pa.int8())
        }

        if not skip_cols:
            pa_cols["SalesOrderNumber"] = pa.array(sales_order_num.tolist(), type=pa.string())
            pa_cols["SalesOrderLineNumber"] = pa.array(line_num, type=pa.int64())

        return pa.table(pa_cols)

    else:
        df = {
            "OrderDate": od_np,
            "DueDate": due_date_np.astype("datetime64[D]"),
            "DeliveryDate": delivery_date_np.astype("datetime64[D]"),
            "StoreKey": store_key_arr,
            "ProductKey": product_keys,
            "PromotionKey": promo_keys,
            "CurrencyKey": currency_arr,
            "CustomerKey": customer_keys,
            "Quantity": qty,
            "NetPrice": final_net_price,
            "UnitCost": final_unit_cost,
            "UnitPrice": final_unit_price,
            "DiscountAmount": final_discount_amt,
            "DeliveryStatus": delivery_status,
            "IsOrderDelayed": is_order_delayed
        }

        if not skip_cols:
            df["SalesOrderNumber"] = sales_order_num.astype(str)
            df["SalesOrderLineNumber"] = line_num

        return pd.DataFrame(df)
