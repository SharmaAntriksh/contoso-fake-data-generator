# ---------------------------------------------------------
#  CURRENCY DIMENSION (PIPELINE READY)
# ---------------------------------------------------------

import pandas as pd
from pathlib import Path

from src.utils.logging_utils import info, fail, skip, stage
from src.pipeline.versioning import should_regenerate, save_version


# ---------------------------------------------------------
# ORIGINAL GENERATOR (UNCHANGED)
# ---------------------------------------------------------

CURRENCY_NAME_MAP = {
    "USD": "US Dollar",
    "EUR": "Euro",
    "INR": "Indian Rupee",
    "GBP": "British Pound",
    "AUD": "Australian Dollar",
    "CAD": "Canadian Dollar",
    "CNY": "Chinese Yuan",
    "JPY": "Japanese Yen",
    "NZD": "New Zealand Dollar",
    "CHF": "Swiss Franc",
    "SEK": "Swedish Krona",
    "NOK": "Norwegian Krone",
    "SGD": "Singapore Dollar",
    "HKD": "Hong Kong Dollar",
    "KRW": "Korean Won",
    "ZAR": "South African Rand",
}


def generate_currency_dimension(currencies):
    """
    Build DimCurrency table.
    'currencies' is the ordered list from config,
    e.g. ["USD", "EUR", "INR", "GBP"]
    """
    iso = list(currencies)

    df = pd.DataFrame({
        "CurrencyKey": range(1, len(iso) + 1),
        "ISOCode": iso,
        "CurrencyName": [
            CURRENCY_NAME_MAP.get(c, c)  # fallback uses ISO
            for c in iso
        ],
    })

    return df


# ---------------------------------------------------------
# PIPELINE ENTRYPOINT
# ---------------------------------------------------------

def run_currency(cfg, parquet_folder: Path):
    """
    Pipeline wrapper for currency dimension.
    Handles regeneration logic, logging, and parquet writing.
    """
    out_path = parquet_folder / "currency.parquet"

    if not should_regenerate("currency", cfg, out_path):
        skip("Currency up-to-date; skipping.")
        return

    # FIX: configuration section is "currencies", NOT "currency"
    currencies = cfg['exchange_rates']["currencies"]

    with stage("Generating Currency"):
        df = generate_currency_dimension(currencies)
        df.to_parquet(out_path, index=False)

    save_version("currency", cfg, out_path)
    info(f"Currency dimension written → {out_path}")
