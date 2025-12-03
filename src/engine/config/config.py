import json
from pathlib import Path

def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Normalize sales section immediately
    cfg["sales"] = normalize_sales_config(cfg["sales"])
    return cfg


def normalize_sales_config(cfg):
    # Remove parquet-only keys when CSV mode is selected
    if cfg.get("file_format") == "csv":
        parquet_only = (
            "row_group_size", "compression",
            "merge_parquet", "merged_file"
        )
        cfg = {k: v for k, v in cfg.items() if k not in parquet_only}

    return cfg


def validate_config(cfg_section, section_name, required_keys):
    for k in required_keys:
        if k not in cfg_section:
            raise KeyError(f"Missing '{k}' in config section '{section_name}'")


def prepare_paths(sales_cfg):
    parquet_dims = Path(sales_cfg["parquet_folder"])
    fact_out = Path(sales_cfg["out_folder"])

    parquet_dims.mkdir(parents=True, exist_ok=True)
    fact_out.mkdir(parents=True, exist_ok=True)

    return parquet_dims, fact_out
