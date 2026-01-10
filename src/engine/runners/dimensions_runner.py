# ---------------------------------------------------------
#  DIMENSIONS ORCHESTRATOR (CLEAN + DATE-AWARE)
# ---------------------------------------------------------
from pathlib import Path
from typing import Dict, Any

from src.dimensions.geography import run_geography
from src.dimensions.customers import run_customers
from src.dimensions.stores import run_stores
from src.dimensions.promotions import run_promotions
from src.dimensions.dates import run_dates
from src.dimensions.currency import run_currency
from src.dimensions.exchange_rates import run_exchange_rates
from src.dimensions.products.products import (
    generate_product_dimension as run_products
)

from src.utils.logging_utils import done, skip


# =========================================================
# Helpers
# =========================================================

def _get_defaults_dates(cfg: Dict[str, Any]):
    """
    Return defaults.dates from cfg.
    Supports both 'defaults' and '_defaults' (backward compatibility).
    """
    defaults_section = cfg.get("defaults") or cfg.get("_defaults")
    if not defaults_section:
        return None
    return defaults_section.get("dates")


def _cfg_with_global_dates(
    cfg: Dict[str, Any],
    dim_key: str,
    global_dates,
):
    """
    Produce a lightweight cfg variant where cfg[dim_key] is augmented
    with a stable 'global_dates' entry.

    IMPORTANT:
    - Only the dimension section is copied.
    - The rest of cfg is reused (no deep copy of entire config).
    """
    if global_dates is None:
        return cfg

    # Shallow copy of root
    cfg_for = cfg.copy()

    # Shallow copy of the dimension section
    dim_section = dict(cfg.get(dim_key, {}))
    dim_section["global_dates"] = global_dates

    cfg_for[dim_key] = dim_section
    return cfg_for


# =========================================================
# Main Orchestrator
# =========================================================

def generate_dimensions(cfg: dict, parquet_dims_folder: Path):
    """
    Orchestrates dimension generation in correct dependency order.

    Guarantees:
    - Date-dependent dimensions regenerate when defaults.dates change
    - Non-date-dependent dimensions are isolated from date changes
    - Dependency order is strictly preserved
    """

    parquet_dims_folder = Path(parquet_dims_folder).resolve()
    parquet_dims_folder.mkdir(parents=True, exist_ok=True)

    # Resolve global default dates ONCE
    global_dates = _get_defaults_dates(cfg)

    # -----------------------------------------------------
    # 1. Geography (root, not date-dependent)
    # -----------------------------------------------------
    run_geography(cfg, parquet_dims_folder)

    # -----------------------------------------------------
    # 2. Customers (depends on geography, not date-dependent)
    # -----------------------------------------------------
    run_customers(cfg, parquet_dims_folder)

    # -----------------------------------------------------
    # 3. Stores (date-dependent)
    # -----------------------------------------------------
    run_stores(
        _cfg_with_global_dates(cfg, "stores", global_dates),
        parquet_dims_folder,
    )

    # -----------------------------------------------------
    # 4. Promotions (date-dependent)
    # -----------------------------------------------------
    run_promotions(
        _cfg_with_global_dates(cfg, "promotions", global_dates),
        parquet_dims_folder,
    )

    # -----------------------------------------------------
    # 5. Products (static; scenario-based, not date-based)
    # -----------------------------------------------------
    products = run_products(cfg, parquet_dims_folder)

    if products.get("_regenerated"):
        done("Generating Product Dimension completed")
    else:
        skip("Product Dimension up-to-date; skipping.")

    # -----------------------------------------------------
    # 6. Dates (date-dependent)
    # -----------------------------------------------------
    run_dates(
        _cfg_with_global_dates(cfg, "dates", global_dates),
        parquet_dims_folder,
    )

    # -----------------------------------------------------
    # 7. Currency (date-dependent)
    # -----------------------------------------------------
    run_currency(
        _cfg_with_global_dates(cfg, "currency", global_dates),
        parquet_dims_folder,
    )

    # -----------------------------------------------------
    # 8. Exchange Rates (date-dependent)
    # -----------------------------------------------------
    run_exchange_rates(
        _cfg_with_global_dates(cfg, "exchange_rates", global_dates),
        parquet_dims_folder,
    )
