import os
import pandas as pd
import numpy as np

# =====================================================================
# CURATED GEOGRAPHY LIST (Option A)
# =====================================================================
CURATED_ROWS = [
    # --- United States (USD) ---
    ("New York", "NY", "United States", "North America", "USD"),
    ("Los Angeles", "CA", "United States", "North America", "USD"),
    ("Chicago", "IL", "United States", "North America", "USD"),
    ("Houston", "TX", "United States", "North America", "USD"),
    ("Miami", "FL", "United States", "North America", "USD"),

    # --- Canada (CAD) ---
    ("Toronto", "ON", "Canada", "North America", "CAD"),
    ("Vancouver", "BC", "Canada", "North America", "CAD"),
    ("Montreal", "QC", "Canada", "North America", "CAD"),

    # --- United Kingdom (GBP) ---
    ("London", "London", "United Kingdom", "Europe", "GBP"),
    ("Manchester", "Manchester", "United Kingdom", "Europe", "GBP"),

    # --- Germany (EUR) ---
    ("Berlin", "Berlin", "Germany", "Europe", "EUR"),
    ("Munich", "Bavaria", "Germany", "Europe", "EUR"),

    # --- France (EUR) ---
    ("Paris", "Île-de-France", "France", "Europe", "EUR"),
    ("Lyon", "Auvergne-Rhône-Alpes", "France", "Europe", "EUR"),

    # --- India (INR) ---
    ("Mumbai", "MH", "India", "Asia", "INR"),
    ("Delhi", "DL", "India", "Asia", "INR"),
    ("Bengaluru", "KA", "India", "Asia", "INR"),

    # --- Australia (AUD) ---
    ("Sydney", "NSW", "Australia", "Oceania", "AUD"),
    ("Melbourne", "VIC", "Australia", "Oceania", "AUD"),
]


# =====================================================================
# BUILD DIM GEOGRAPHY (curated + weighted sampling)
# =====================================================================
def build_dim_geography(cfg):
    """
    Build curated geography dimension limited by allowed currencies and weighted
    per-country proportions defined in config["geography"]["country_weights"].
    """

    allowed_iso = set(cfg["exchange_rates"]["currencies"])
    gcfg = cfg["geography"]
    target_rows = gcfg.get("target_rows", 200)

    # ------------------------------------------------------------
    # Convert curated rows → DataFrame
    # ------------------------------------------------------------
    df = pd.DataFrame(
        CURATED_ROWS,
        columns=["City", "State", "Country", "Continent", "ISOCode"],
    )

    # ------------------------------------------------------------
    # Filter by allowed currency ISO codes
    # ------------------------------------------------------------
    before = len(df)
    df = df[df["ISOCode"].isin(allowed_iso)].reset_index(drop=True)
    after = len(df)

    if after == 0:
        raise ValueError(
            f"No geography rows remain after filtering by allowed currencies: {sorted(allowed_iso)}"
        )

    if before != after:
        removed = before - after
        print(f"[geography] Removed {removed} rows due to unsupported currencies.")
        print(f"[geography] Allowed: {sorted(allowed_iso)}")

    # ------------------------------------------------------------
    # Weighting per country based on config
    # ------------------------------------------------------------
    country_weights = gcfg.get("country_weights", {})

    # Normalize weights if they sum incorrectly
    total_w = sum(country_weights.values())
    if total_w > 0 and total_w != 1.0:
        for k in country_weights:
            country_weights[k] /= total_w

    # Assign weight per row
    df["Weight"] = df["Country"].apply(
        lambda c: country_weights.get(c, country_weights.get("Rest", 0))
    )

    # Normalize row weights to 1.0
    wsum = df["Weight"].sum()
    if wsum == 0:
        raise ValueError("All country weights resolved to zero. Check config.")
    df["Weight"] = df["Weight"] / wsum

    # ------------------------------------------------------------
    # Weighted sampling with replacement
    # ------------------------------------------------------------
    sampled = df.sample(
        n=target_rows,
        replace=True,
        weights=df["Weight"],
        random_state=42,
    ).reset_index(drop=True)

    # Assign deterministic keys
    sampled["GeographyKey"] = np.arange(1, target_rows + 1)

    # ------------------------------------------------------------
    # Write parquet
    # ------------------------------------------------------------
    output = "./data/parquet_dims/geography.parquet"
    os.makedirs(os.path.dirname(output), exist_ok=True)

    sampled.to_parquet(output, index=False)
    print(f"[geography] Saved {len(sampled)} rows → {output}")

    return sampled
