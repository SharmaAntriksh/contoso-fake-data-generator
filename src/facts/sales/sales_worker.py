import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logging_utils import work


# ======================================================================================
# GLOBALS: These must MATCH EXACTLY what sales.py passes in initargs
# ======================================================================================

_G_product_np = None
_G_store_keys = None
_G_promo_keys_all = None
_G_promo_pct_all = None
_G_promo_start_all = None
_G_promo_end_all = None
_G_customers = None
_G_store_to_geo = None
_G_geo_to_currency = None
_G_date_pool = None
_G_date_prob = None

_G_out_folder = None
_G_file_format = None
_G_row_group_size = 250_000
_G_compression = "lz4"

_G_no_discount_key = None
_G_delta_output_folder = None
_G_write_delta = False
_G_skip_order_cols = False

_G_partition_enabled = False
_G_partition_cols = None


# ======================================================================================
# INITIALIZER (must match argument order used in sales.py)
# ======================================================================================

def init_sales_worker(
    product_np,
    store_keys,
    promo_keys_all,
    promo_pct_all,
    promo_start_all,
    promo_end_all,
    customers,
    store_to_geo,
    geo_to_currency,
    date_pool,
    date_prob,
    out_folder,
    file_format,
    row_group_size,
    compression,
    no_discount_key,
    delta_output_folder,
    write_delta,
    skip_order_cols,
    partition_enabled,
    partition_cols,
):

    global _G_product_np, _G_store_keys, _G_promo_keys_all, _G_promo_pct_all, \
           _G_promo_start_all, _G_promo_end_all, _G_customers, _G_store_to_geo, \
           _G_geo_to_currency, _G_date_pool, _G_date_prob, _G_out_folder, \
           _G_file_format, _G_row_group_size, _G_compression, _G_no_discount_key, \
           _G_delta_output_folder, _G_write_delta, _G_skip_order_cols, \
           _G_partition_enabled, _G_partition_cols

    _G_product_np = product_np
    _G_store_keys = store_keys
    _G_promo_keys_all = promo_keys_all
    _G_promo_pct_all = promo_pct_all
    _G_promo_start_all = promo_start_all
    _G_promo_end_all = promo_end_all
    _G_customers = customers
    _G_store_to_geo = store_to_geo
    _G_geo_to_currency = geo_to_currency
    _G_date_pool = date_pool
    _G_date_prob = date_prob

    _G_out_folder = out_folder
    _G_file_format = file_format
    _G_row_group_size = row_group_size
    _G_compression = compression

    _G_no_discount_key = no_discount_key
    _G_delta_output_folder = delta_output_folder
    _G_write_delta = write_delta
    _G_skip_order_cols = skip_order_cols

    _G_partition_enabled = partition_enabled
    _G_partition_cols = partition_cols


# ======================================================================================
# OPTIMIZED PARQUET WRITER (fast, low-memory)
# ======================================================================================
def write_parquet_optimized(table: pa.Table, path: str):
    """
    Optimized Parquet writer for large output:
    - row-group streaming
    - compression (lz4 / zstd)
    - no dict encoding
    - no statistics
    """
    # Partition columns (optional, re-order to end)
    part_cols = [c for c in ("Year", "Month") if c in table.column_names]
    normal_cols = [c for c in table.column_names if c not in part_cols]
    table = table.select(normal_cols + part_cols)

    writer = pq.ParquetWriter(
        path,
        table.schema,
        compression=_G_compression,
        use_dictionary=False,
        write_statistics=False
    )

    for batch in table.to_batches(max_chunksize=_G_row_group_size):
        writer.write_batch(batch)

    writer.close()


def _worker_task(task):
    idx, rows, seed, _ = task
    rng = np.random.default_rng(seed)

    # --- Dates ---
    order_dates = rng.choice(_G_date_pool, size=rows, p=_G_date_prob)
    order_dates = np.asarray(order_dates).reshape(-1)

    # --- Store → Geo → Currency ---
    stores = rng.choice(_G_store_keys, rows)
    stores = np.asarray(stores).reshape(-1)

    geos = np.array([int(_G_store_to_geo[int(s)]) for s in stores], dtype=np.int32)

    currencies = np.array([int(_G_geo_to_currency[int(g)]) for g in geos], dtype=np.int16)

    # --- Customers ---
    customers = rng.choice(_G_customers, rows)
    customers = np.asarray(customers).reshape(-1)

    # --- Products ---
    products = rng.choice(_G_product_np.reshape(-1), rows)

    # --- Build DF (now all 1-D) ---
    df = pd.DataFrame({
        "OrderDate": order_dates,
        "DueDate": order_dates,
        "DeliveryDate": order_dates,

        "StoreKey": stores,
        "ProductKey": products,
        "PromotionKey": rng.choice(_G_promo_keys_all, rows),
        "CurrencyKey": currencies,
        "CustomerKey": customers,

        "Quantity": rng.integers(1, 5, rows),
        "NetPrice": rng.uniform(1, 100, rows).round(2),
        "UnitCost": rng.uniform(1, 50, rows).round(2),
        "UnitPrice": rng.uniform(1, 120, rows).round(2),
        "DiscountAmount": rng.uniform(0, 10, rows).round(2),

        "DeliveryStatus": rng.choice(["On Time", "Delayed"], rows, p=[0.9, 0.1]),
        "IsOrderDelayed": rng.choice([0, 1], rows, p=[0.9, 0.1]),
    })


    # ------------------------------------------------------------------
    # PARTITION COLUMNS (Year / Month)
    # ------------------------------------------------------------------
    df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")
    df = df[df["OrderDate"].notna()]

    if _G_partition_enabled:
        df["Year"] = df["OrderDate"].dt.year.astype("int16")
        df["Month"] = df["OrderDate"].dt.month.astype("int8")

    # ------------------------------------------------------------------
    # ARROW CONVERSION + WRITE
    # ------------------------------------------------------------------
    table = pa.Table.from_pandas(df, preserve_index=False)

    out = os.path.join(_G_out_folder, f"sales_chunk{idx:04d}.parquet")
    write_parquet_optimized(table, out)

    work(f"Chunk {idx} → {out}")

    return {
        "chunk": idx,
        "rows": len(df),
        "path": out,
    }
