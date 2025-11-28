# sales_worker.py
import os
import numpy as np
import pandas as pd
import pyarrow as pa
import csv
from deltalake import write_deltalake

from src.utils.logging_utils import work
from .sales_logic import _build_chunk_table, bind_globals


# Globals hydrated at init
_G_out_folder = None
_G_file_format = None
_G_row_group_size = None
_G_compression = None
_G_no_discount_key = None
_G_delta_output_folder = None
_G_write_delta = None


def _init_worker(init_args):
    """
    Assign globals for this worker process and bind globals to sales_logic.
    Ensures _build_chunk_table sees the exact same inputs as the old monolithic sales.py.
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
        skip_flag
    ) = init_args

    # Bind globals in THIS module
    globals().update(dict(
        _G_out_folder=out_folder,
        _G_file_format=file_format,
        _G_row_group_size=row_group_size,
        _G_compression=compression,
        _G_no_discount_key=no_discount_key,
        _G_delta_output_folder=delta_output_folder,
        _G_write_delta=write_delta
    ))

    # Bind directly into sales_logic
    bind_globals(dict(
        _G_skip_order_cols=skip_flag,
        _G_product_np=product_np,
        _G_customers=customers,
        _G_date_pool=date_pool,
        _G_date_prob=date_prob,
        _G_store_keys=store_keys,
        _G_promo_keys_all=promo_keys_all,
        _G_promo_pct_all=promo_pct_all,
        _G_promo_start_all=promo_start_all,
        _G_promo_end_all=promo_end_all,
        _G_store_to_geo=store_to_geo,
        _G_geo_to_currency=geo_to_currency
    ))

    # Precompute store→geo and geo→currency arrays for vectorized lookups
    try:
        if store_to_geo:
            max_store = max(store_to_geo.keys())
            arr = np.full(max_store + 1, -1, dtype=np.int64)
            for k, v in store_to_geo.items():
                arr[k] = v
            bind_globals({"_G_store_to_geo_arr": arr})

        if geo_to_currency:
            max_geo = max(geo_to_currency.keys())
            arr = np.full(max_geo + 1, -1, dtype=np.int64)
            for k, v in geo_to_currency.items():
                arr[k] = v
            bind_globals({"_G_geo_to_currency_arr": arr})

    except Exception:
        # If anything goes wrong, fall back to dict lookups in the logic module
        bind_globals({"_G_store_to_geo_arr": None})
        bind_globals({"_G_geo_to_currency_arr": None})


def _worker_task(args):
    """
    Worker entry point: generate chunk using logic module, then write to parquet/csv.
    """
    idx, batch, total_chunks, seed = args

    # generate
    table_or_df = _build_chunk_table(batch, seed, no_discount_key=_G_no_discount_key)

    # unify to arrow
    if isinstance(table_or_df, pa.Table):
        table_for_arrow = table_or_df
    else:
        table_for_arrow = pa.Table.from_pandas(table_or_df, preserve_index=False)

    # output
    if _G_file_format == "csv":
        out = os.path.join(_G_out_folder, f"sales_chunk{idx:04d}.csv")
        df = table_for_arrow.to_pandas()
        df.to_csv(out, index=False, quoting=csv.QUOTE_ALL)

    elif _G_file_format == "deltaparquet":
        # return to parent for delta write (because Delta Lake writes must be done in main proc)
        return ("delta", idx, table_for_arrow)

    else:
        import pyarrow.parquet as pq
        out = os.path.join(_G_out_folder, f"sales_chunk{idx:04d}.parquet")
        pq.write_table(
            table_for_arrow,
            out,
            row_group_size=_G_row_group_size,
            compression=_G_compression
        )


        # OPTIONAL: also write Delta from worker (if user enabled it)
        if _G_write_delta:
            write_deltalake(_G_delta_output_folder, table_for_arrow, mode="append")

    pct = int((idx + 1) / total_chunks * 100)
    work(f"Chunk {idx+1}/{total_chunks} ({pct}%) -> {out}")

    return out
