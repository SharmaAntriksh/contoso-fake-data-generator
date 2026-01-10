import json
from pathlib import Path
from typing import Dict, Iterable


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------

def load_config(path: str | Path = "config.json") -> dict:
    """
    Load a JSON config file and normalize sections safely.
    This function MUST NOT silently drop user-defined keys.
    """
    path = Path(path)

    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    if "sales" not in cfg or not isinstance(cfg["sales"], dict):
        raise KeyError("Missing or invalid 'sales' section in config")

    cfg["sales"] = normalize_sales_config(cfg["sales"])
    return cfg


# ------------------------------------------------------------
# Sales config normalization
# ------------------------------------------------------------

def normalize_sales_config(sales_cfg: Dict) -> Dict:
    """
    Normalize sales configuration without destructive mutation.

    Rules:
    - CSV mode ignores parquet-only options (but does not delete them)
    - Required keys must exist
    - Flags like skip_order_cols are preserved verbatim
    """
    sales_cfg = dict(sales_cfg)  # shallow copy (no side effects)

    file_format = sales_cfg.get("file_format")
    if not file_format:
        raise KeyError("sales.file_format is required")

    file_format = file_format.lower()
    sales_cfg["file_format"] = file_format

    _validate_required_keys(
        sales_cfg,
        section="sales",
        required=("parquet_folder", "out_folder", "total_rows"),
    )

    # CSV mode: parquet-only options are ignored, not removed
    if file_format == "csv":
        sales_cfg.setdefault("_ignored_keys", [])
        sales_cfg["_ignored_keys"].extend(
            k for k in _PARQUET_ONLY_KEYS if k in sales_cfg
        )

    return sales_cfg


# ------------------------------------------------------------
# Validation helpers
# ------------------------------------------------------------

def _validate_required_keys(
    cfg: Dict,
    section: str,
    required: Iterable[str],
) -> None:
    missing = [k for k in required if k not in cfg]
    if missing:
        raise KeyError(
            f"Missing required keys in '{section}' config: {missing}"
        )


# ------------------------------------------------------------
# Path preparation
# ------------------------------------------------------------

def prepare_paths(sales_cfg: Dict):
    """
    Prepare and validate filesystem paths used by sales pipeline.
    """
    parquet_dims = Path(sales_cfg["parquet_folder"]).resolve()
    fact_out = Path(sales_cfg["out_folder"]).resolve()

    parquet_dims.mkdir(parents=True, exist_ok=True)
    fact_out.mkdir(parents=True, exist_ok=True)

    return parquet_dims, fact_out


# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------

_PARQUET_ONLY_KEYS = {
    "row_group_size",
    "compression",
    "merge_parquet",
    "merged_file",
}
