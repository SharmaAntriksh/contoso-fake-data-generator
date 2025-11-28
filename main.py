from src.pipeline.config import load_config, validate_config, prepare_paths
from src.pipeline.dimensions import generate_dimensions
from src.pipeline.sales_pipeline import run_sales_pipeline
from src.pipeline.packaging import package_output


def main():
    # ---------------------------------------------------------
    # Load + validate config
    # ---------------------------------------------------------
    cfg = load_config()
    sales_cfg = cfg["sales"]

    validate_config(
        sales_cfg,
        "sales",
        ["total_rows", "chunk_size", "file_format"]
    )

    # ---------------------------------------------------------
    # Prepare folder paths (parquet dims + fact out)
    # ---------------------------------------------------------
    parquet_dims, fact_out = prepare_paths(sales_cfg)

    # ---------------------------------------------------------
    # Generate dimension tables
    # ---------------------------------------------------------
    generate_dimensions(cfg, parquet_dims)

    # ---------------------------------------------------------
    # Generate Sales Fact
    # ---------------------------------------------------------
    run_sales_pipeline(sales_cfg, fact_out)

    # ---------------------------------------------------------
    # Package final output (dims + facts + SQL scripts)
    # ---------------------------------------------------------
    package_output(cfg, sales_cfg, parquet_dims, fact_out)


if __name__ == "__main__":
    main()
