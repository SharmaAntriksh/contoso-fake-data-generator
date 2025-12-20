from __future__ import annotations

import time
from pathlib import Path
import shutil

from src.utils.logging_utils import stage, skip, info, done
from src.engine.dimension_loader import load_dimension
from src.versioning.version_store import should_regenerate, save_version
from src.engine.packaging import package_output


def run_sales_pipeline(sales_cfg, fact_out, parquet_dims, cfg):
    """Run the sales fact pipeline with correct deltaparquet handling."""

    # ------------------------------------------------------------
    # Resolve and normalize key paths
    # ------------------------------------------------------------
    fact_out = Path(fact_out).resolve()
    parquet_dims = Path(parquet_dims).resolve()

    fact_out.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------
    # Dimension dependencies
    # ------------------------------------------------------------
    def changed(name, section_cfg):
        dim_file = parquet_dims / f"{name}.parquet"
        return should_regenerate(name, section_cfg, dim_file)

    info("Sales will regenerate (forced).")

    # ------------------------------------------------------------
    # Delta output folder
    # ------------------------------------------------------------
    # Decide actual output folder based on file_format
    fmt = sales_cfg["file_format"].lower()

    if fmt == "csv":
        sales_out_folder = Path(fact_out) / "csv"
    elif fmt == "parquet":
        sales_out_folder = Path(fact_out) / "parquet"
    else:  # deltaparquet
        delta_raw = sales_cfg.get("delta_output_folder", "delta")
        sales_out_folder = Path(delta_raw).expanduser().resolve()

    # Clean output folder every run
    if sales_out_folder.exists():
        shutil.rmtree(sales_out_folder, ignore_errors=True)
    sales_out_folder.mkdir(parents=True, exist_ok=True)

    # info(f"Resolved sales output folder = {sales_out_folder}")
    # print("SALES CFG PRICING →", sales_cfg.get("pricing"))
    # ------------------------------------------------------------
    # Bind PRICING config into flat State (REQUIRED)
    # ------------------------------------------------------------
    from src.facts.sales.sales_logic.globals import State

    pricing = sales_cfg.get("pricing", {})   # ✅ FIXED
    decimals = pricing.get("decimals", {})

    # ---- pricing ----
    State.pricing_mode = pricing.get("pricing_mode", "random")
    State.enforce_min_price = pricing.get("enforce_min_price", False)
    State.bucket_size = pricing.get("bucket_size", 0.25)
    State.discrete_factors = pricing.get("discrete_factors", False)

    # ---- decimals (FLAT) ----
    State.decimals_mode = decimals.get("mode", "off")
    State.decimals_scale = decimals.get("scale", 0.02)

    # ---- retail endings ----
    State.retail_price_endings = pricing.get("retail_price_endings", False)

    info(
        f"Pricing bound → mode={State.pricing_mode}, "
        f"decimals={State.decimals_mode}, "
        f"retail_endings={State.retail_price_endings}"
    )


    # ------------------------------------------------------------
    # Run sales fact builder
    # ------------------------------------------------------------
    from src.facts.sales.sales import generate_sales_fact

    parquet_folder = parquet_dims

    stage("Generating Sales")
    t0 = time.time()

    generate_sales_fact(
        cfg,
        parquet_folder=str(parquet_folder),
        out_folder=str(sales_out_folder),             # ✔ Corrected
        total_rows=sales_cfg["total_rows"],
        file_format=sales_cfg["file_format"],
        row_group_size=sales_cfg.get("row_group_size", 2000000),
        compression=sales_cfg.get("compression", "snappy"),
        chunk_size=sales_cfg.get("chunk_size", 1000000),
        workers=sales_cfg.get("workers", None),
        partition_enabled=sales_cfg.get("partition_enabled", False),
        partition_cols=sales_cfg.get("partition_cols", ["Year", "Month"]),
        delta_output_folder=str(sales_out_folder),
        skip_order_cols=sales_cfg.get("skip_order_cols", False)
    )

    done(f"Generating Sales completed in {time.time() - t0:.1f}s")

    # ------------------------------------------------------------
    # Packaging
    # ------------------------------------------------------------

    stage("Creating Final Output Folder")
    t1 = time.time()
    package_output(cfg, sales_cfg, parquet_dims, fact_out)
    done(f"Creating Final Output Folder completed in {time.time() - t1:.1f}s")


