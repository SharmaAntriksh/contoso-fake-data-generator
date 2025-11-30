import os
import numpy as np
import pandas as pd
import pyarrow as pa
import csv
from deltalake import write_deltalake

from src.utils.logging_utils import work
from .sales_logic import _build_chunk_table, bind_globals


# =====================================================================
# GLOBALS SHARED BY EACH WORKER
# =====================================================================
_G_out_folder = None
_G_file_format = None
_G_row_group_size = None
_G_compression = None
_G_no_discount_key = None
_G_delta_output_folder = None
_G_write_delta = None

# NEW — worker-level partitioning
_G_partition_enabled = False
_G_partition_cols = None


# =====================================================================
# WORKER INITIALIZER
# =====================================================================
def _init_worker(init_args):
    """
    Workers receive all data needed by sales_logic and write configuration.
    """

    (
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
        skip_flag,

        # NEW — partitioning config injected by pipeline/sales.py
        partition_enabled,
        partition_cols,
    ) = init_args

    # -----------------------------
    # Bind worker-local globals
    # -----------------------------
    globals().update(dict(
        _G_out_folder=out_folder,
        _G_file_format=file_format,
        _G_row_group_size=row_group_size,
        _G_compression=compression,
        _G_no_discount_key=no_discount_key,
        _G_delta_output_folder=delta_output_folder,
        _G_write_delta=write_delta,

        # NEW
        _G_partition_enabled=partition_enabled,
        _G_partition_cols=partition_cols,
    ))

    # -----------------------------
    # Bind globals into sales_logic
    # -----------------------------
    bind_globals({
        "_G_skip_order_cols":  skip_flag,
        "_G_product_np":       product_np,
        "_G_customers":        customers,
        "_G_date_pool":        date_pool,
        "_G_date_prob":        date_prob,
        "_G_store_keys":       store_keys,
        "_G_promo_keys_all":   promo_keys_all,
        "_G_promo_pct_all":    promo_pct_all,
        "_G_promo_start_all":  promo_start_all,
        "_G_promo_end_all":    promo_end_all,
        "_G_store_to_geo":     store_to_geo,
        "_G_geo_to_currency":  geo_to_currency,
    })

    # -----------------------------
    # Array-optimize store→geo, geo→currency
    # -----------------------------
    try:
        if store_to_geo:
            max_store = max(store_to_geo.keys())
            arr = np.full(max_store + 1, -1, dtype=np.int64)
            for k, v in store_to_geo.items():
                arr[k] = v
            bind_globals({"_G_store_to_geo_arr": arr})
        else:
            bind_globals({"_G_store_to_geo_arr": None})

        if geo_to_currency:
            max_geo = max(geo_to_currency.keys())
            arr = np.full(max_geo + 1, -1, dtype=np.int64)
            for k, v in geo_to_currency.items():
                arr[k] = v
            bind_globals({"_G_geo_to_currency_arr": arr})
        else:
            bind_globals({"_G_geo_to_currency_arr": None})

    except Exception:
        bind_globals({"_G_store_to_geo_arr": None})
        bind_globals({"_G_geo_to_currency_arr": None})


# =====================================================================
# WORKER TASK  (FULLY UPDATED)
# =====================================================================
def _worker_task(args):
    """
    args = (idx, batch_size, total_chunks, seed)
    Builds chunk, writes output (CSV or Parquet), returns path.
    NOTE: For deltaparquet we WRITE PARQUET CHUNKS (no delta writes in workers).
    """
    idx, batch, total_chunks, seed = args

    # -----------------------------
    # Build chunk
    # -----------------------------
    table_or_df = _build_chunk_table(batch, seed, no_discount_key=_G_no_discount_key)

    # Normalize to Arrow table
    if isinstance(table_or_df, pa.Table):
        table = table_or_df
    else:
        table = pa.Table.from_pandas(table_or_df, preserve_index=False)

    # -----------------------------
    # Write output by format
    # -----------------------------
    if _G_file_format == "csv":
        out = os.path.join(_G_out_folder, f"sales_chunk{idx:04d}.csv")
        df = table.to_pandas()
        df.to_csv(out, index=False, quoting=csv.QUOTE_ALL)

    else:
        # For both 'parquet' and 'deltaparquet' workers write parquet chunks.
        # If partitioning is enabled for deltaparquet, inject Year/Month columns first.
        out = os.path.join(_G_out_folder, f"sales_chunk{idx:04d}.parquet")

        if _G_file_format == "deltaparquet" and _G_partition_enabled and _G_partition_cols:
            # we need partition columns present in the chunk before main append.
            df = table.to_pandas()

            # ensure OrderDate is datetime
            if "OrderDate" in df.columns:
                df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")

            if "Year" in _G_partition_cols and "Year" not in df.columns:
                if "OrderDate" in df.columns:
                    df["Year"] = df["OrderDate"].dt.year
                else:
                    df["Year"] = None

            if "Month" in _G_partition_cols and "Month" not in df.columns:
                if "OrderDate" in df.columns:
                    df["Month"] = df["OrderDate"].dt.month
                else:
                    df["Month"] = None

            # convert back to arrow (preserve types)
            table = pa.Table.from_pandas(df, preserve_index=False)

        # write parquet via pyarrow
        import pyarrow.parquet as pq
        pq.write_table(
            table,
            out,
            row_group_size=_G_row_group_size,
            compression=_G_compression,
        )

        # optional: if _G_write_delta is set and you want immediate single-file delta behavior,
        # we do NOT call write_deltalake here — main process will handle delta merge.

    # -----------------------------
    # Progress indicator
    # -----------------------------
    work(chunk=idx + 1, total=total_chunks, outfile=out)

    return out
