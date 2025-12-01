import json
from pathlib import Path

from src.pipeline.dimensions import generate_dimensions
from src.pipeline.sales_pipeline import run_sales_pipeline

from src.utils.logging_utils import info, fail


def main():
    try:
        # ------------------------------------------------------------
        # Load configuration
        # ------------------------------------------------------------
        cfg_path = Path("config.json")
        if not cfg_path.exists():
            raise FileNotFoundError("config.json not found.")

        with cfg_path.open("r") as f:
            cfg = json.load(f)

        sales_cfg = cfg["sales"]

        parquet_dims = Path(sales_cfg["parquet_folder"])
        fact_out = Path(sales_cfg["out_folder"])


        # ------------------------------------------------------------
        # Generate dimensions (with versioning skip logic)
        # ------------------------------------------------------------
        generate_dimensions(cfg, parquet_dims)

        # ------------------------------------------------------------
        # Generate Sales Fact (includes its own packaging logic)
        # ------------------------------------------------------------
        run_sales_pipeline(sales_cfg, fact_out, parquet_dims, cfg)

        # ------------------------------------------------------------
        # DO NOT PACKAGE AGAIN — this caused deltaparquet failures
        # package_output(cfg, sales_cfg, parquet_dims, fact_out)
        # ------------------------------------------------------------

        info("All pipelines complete.")

    except Exception as ex:
        fail(str(ex))
        raise


if __name__ == "__main__":
    main()
