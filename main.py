from pathlib import Path
import time

from src.engine.dimensions_runner import generate_dimensions
from src.engine.sales_runner import run_sales_pipeline
from src.utils.logging_utils import info, fail, PIPELINE_START_TIME, fmt_sec
from src.engine.config_loader import load_config_file, load_config


def main():
    try:
        # Load config (json/yaml auto-detect)
        raw_cfg = load_config_file("config.yaml")
        cfg = load_config(raw_cfg)

        sales_cfg = cfg["sales"]
        parquet_dims = Path(sales_cfg.get("parquet_folder", "./data/parquet_dims"))
        fact_out = Path(sales_cfg["out_folder"])

        generate_dimensions(cfg, parquet_dims)
        run_sales_pipeline(sales_cfg, fact_out, parquet_dims, cfg)

        info("All pipelines complete.")

        elapsed = time.time() - PIPELINE_START_TIME
        info(f"Total pipeline time: {fmt_sec(elapsed)}")

    except Exception as ex:
        fail(str(ex))
        raise


if __name__ == "__main__":
    main()
