# ---------------------------------------------------------
#  DIMENSIONS ORCHESTRATOR (CLEAN + MINIMAL)
# ---------------------------------------------------------

from pathlib import Path

from src.pipeline.dimensions.geography import run_geography
from src.pipeline.dimensions.customers import run_customers
from src.pipeline.dimensions.stores import run_stores
from src.pipeline.dimensions.promotions import run_promotions
from src.pipeline.dimensions.dates import run_dates
from src.pipeline.dimensions.currency import run_currency
from src.pipeline.dimensions.exchange_rates import run_exchange_rates


def generate_dimensions(cfg: dict, parquet_dims_folder: Path):
    """
    Orchestrates dimension generation in correct dependency order.
    Each dimension handles its own regeneration/versioning logic.
    
    Dependency Order:
    - Geography → Customers, Stores
    - Promotions → no dependency
    - Dates → required by Sales
    - Currency + FX → independent but FX uses master file
    """

    # 1️⃣ Geography (root)
    run_geography(cfg, parquet_dims_folder)

    # 2️⃣ Customers (depends on geography)
    run_customers(cfg, parquet_dims_folder)

    # 3️⃣ Stores (depends on geography)
    run_stores(cfg, parquet_dims_folder)

    # 4️⃣ Promotions (independent)
    run_promotions(cfg, parquet_dims_folder)

    # 5️⃣ Dates (independent; required for sales)
    run_dates(cfg, parquet_dims_folder)

    # 6️⃣ Currency (independent)
    run_currency(cfg, parquet_dims_folder)

    # 7️⃣ Exchange Rates (independent)
    run_exchange_rates(cfg, parquet_dims_folder)
