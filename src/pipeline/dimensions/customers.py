# ---------------------------------------------------------
#  CUSTOMERS DIMENSION (PIPELINE READY)
# ---------------------------------------------------------

import random
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

from src.utils.logging_utils import info, fail, skip, stage
from src.pipeline.versioning import should_regenerate, save_version
from src.pipeline.dimension_loader import load_dimension


# ---------------------------------------------------------
#  GENERATION HELPERS  (your original functions)
# ---------------------------------------------------------

def load_list(file_path):
    """Utility to load a list from a text or CSV file."""
    try:
        return pd.read_csv(file_path, header=None)[0].tolist()
    except Exception as e:
        fail(f"Failed to load list from {file_path}: {e}")
        raise


def load_real_geography(config):
    """Attempt to load geography from parquet; fallback to CSV."""
    try:
        return load_dimension("geography", config)
    except Exception:
        path = config["customers"]["paths"]["geography"]
        return pd.read_parquet(path)


# ---------------------------------------------------------
#  CORE CUSTOMER GENERATION  (your existing logic)
# ---------------------------------------------------------

def generate_synthetic_customers(config):
    customers_cfg = config["customers"]

    seed = customers_cfg["seed"]
    random.seed(seed)
    np.random.seed(seed)

    names_folder = Path(customers_cfg["names_folder"])

    # ---------------------------------------------------------
    # Load FIRST names from available regional files
    # ---------------------------------------------------------
    eu_first     = load_list(names_folder / "eu_first.csv")
    india_first  = load_list(names_folder / "india_first.csv")
    us_first     = (
        load_list(names_folder / "us_male_first.csv")
        + load_list(names_folder / "us_female_first.csv")
    )

    first_names = eu_first + india_first + us_first

    # ---------------------------------------------------------
    # Load LAST names from available regional files
    # ---------------------------------------------------------
    eu_last     = load_list(names_folder / "eu_last.csv")
    india_last  = load_list(names_folder / "india_last.csv")
    us_last     = load_list(names_folder / "us_surnames.csv")

    last_names = eu_last + india_last + us_last

    # Load geography
    geography = load_real_geography(config)

    pct_india = customers_cfg["pct_india"]
    pct_us = customers_cfg["pct_us"]
    pct_eu = customers_cfg["pct_eu"]
    pct_org = customers_cfg["pct_org"]
    total_customers = customers_cfg["total_customers"]

    count_india = int(total_customers * pct_india)
    count_us = int(total_customers * pct_us)
    count_eu = int(total_customers * pct_eu)
    count_org = int(total_customers * pct_org)

    cust_geo = {
        "India": geography[geography["Country"] == "India"],
        "US": geography[geography["Country"] == "United States"],
        "EU": geography[geography["Country"].isin(["Germany", "France", "Spain", "Italy"])],
    }

    def create_customers(count, region):
        chosen = pd.DataFrame({
            "CustomerKey": range(count),
            "FirstName": np.random.choice(first_names, count),
            "LastName": np.random.choice(last_names, count),
            "Email": [
                f"{fn.lower()}.{ln.lower()}@example.com"
                for fn, ln in zip(
                    np.random.choice(first_names, count),
                    np.random.choice(last_names, count)
                )
            ],
        })

        geo_df = cust_geo.get(region)
        if geo_df is None or geo_df.empty:
            fail(f"No geography for region: {region}")
            raise ValueError(f"No geography entries for region: {region}")

        idx = np.random.randint(0, len(geo_df), count)
        selected_geo = geo_df.iloc[idx]

        chosen["StoreCity"] = selected_geo["City"].values
        chosen["StoreState"] = selected_geo["State"].values
        chosen["StoreCountry"] = selected_geo["Country"].values

        return chosen

    customers_list = [
        create_customers(count_india, "India"),
        create_customers(count_us, "US"),
        create_customers(count_eu, "EU"),
    ]

    if count_org > 0:
        org_df = pd.DataFrame({
            "CustomerKey": range(count_org),
            "FirstName": ["Org"] * count_org,
            "LastName": [f"Corp{i}" for i in range(count_org)],
            "Email": [f"contact{i}@organization.com" for i in range(count_org)],
            "StoreCity": ["" for _ in range(count_org)],
            "StoreState": ["" for _ in range(count_org)],
            "StoreCountry": ["" for _ in range(count_org)],
        })
        customers_list.append(org_df)

    df = pd.concat(customers_list, ignore_index=True)

    # Ensure unique CustomerKey across all generated rows
    df = df.reset_index(drop=True)
    df["CustomerKey"] = (df.index + 1).astype(int)

    df["DateOfBirth"] = [
        datetime(
            random.randint(1950, 2000),
            random.randint(1, 12),
            random.randint(1, 28)
        )
        for _ in range(len(df))
    ]
    df["Age"] = datetime.now().year - df["DateOfBirth"].dt.year

    df["MaritalStatus"] = np.random.choice(["Single", "Married", "Divorced", "Widowed"], len(df))
    df["NumberChildrenAtHome"] = np.random.randint(0, 4, size=len(df))
    df["TotalChildren"] = df["NumberChildrenAtHome"] + np.random.randint(0, 2, size=len(df))
    df["YearlyIncome"] = np.random.randint(20000, 150000, size=len(df))
    df["Education"] = np.random.choice(
        ["High School", "Bachelor", "Master", "PhD"], len(df)
    )

    return df


# ---------------------------------------------------------
#  PUBLIC PIPELINE ENTRYPOINT
# ---------------------------------------------------------

def run_customers(cfg, parquet_folder: Path):
    """
    Pipeline-aware wrapper around customer generation.
    Handles:
    - regeneration checks
    - logging
    - writing parquet
    - versioning
    """

    out_path = parquet_folder / "customers.parquet"

    # Version Check
    if not should_regenerate("customers", cfg, out_path):
        skip("Customers up-to-date; skipping.")
        return

    with stage("Generating Customers"):
        df = generate_synthetic_customers(cfg)
        df.to_parquet(out_path, index=False)

    save_version("customers", cfg, out_path)
    info(f"Customers dimension generated → {out_path}")
