# ---------------------------------------------------------
#  CUSTOMERS DIMENSION (PIPELINE READY)
# ---------------------------------------------------------

import numpy as np
import pandas as pd
from pathlib import Path

from src.utils.logging_utils import info, skip, stage
from src.pipeline.versioning import should_regenerate, save_version
from src.pipeline.dimension_loader import load_dimension


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def load_name_list(path: Path) -> list:
    return pd.read_csv(path, header=None)[0].tolist()


def random_choice(arr, size, rng):
    return rng.choice(arr, size=size)


# ---------------------------------------------------------
# Region-wise generation
# ---------------------------------------------------------

def generate_region_customers(
    region_name: str,
    count: int,
    geography: pd.DataFrame,
    names_folder: Path,
    rng: np.random.Generator,
    pct_org: float
):
    """Generate customers for a single region."""
    if count == 0:
        return pd.DataFrame()

    # ------------------- Names -------------------
    if region_name == "United States":
        male_first  = load_name_list(names_folder / "us_male_first.csv")
        female_first = load_name_list(names_folder / "us_female_first.csv")
        last_names  = load_name_list(names_folder / "us_surnames.csv")

        genders = rng.choice(["M", "F"], size=count)
        first_names = [
            rng.choice(male_first) if g == "M" else rng.choice(female_first)
            for g in genders
        ]
        last_names = random_choice(last_names, count, rng)

    else:
        first_list = load_name_list(names_folder / f"{region_name.lower()}_first.csv")
        last_list  = load_name_list(names_folder / f"{region_name.lower()}_last.csv")
        genders    = rng.choice(["M", "F"], size=count)
        first_names = random_choice(first_list, count, rng)
        last_names  = random_choice(last_list, count, rng)

    # ------------------- Organization flag -------------------
    is_org = rng.random(count) < pct_org
    company_names = pd.Series([f"{region_name} Corp {i}" for i in range(count * 2)])
    org_names = random_choice(company_names, count, rng)
    person_full = pd.Series(first_names) + " " + pd.Series(last_names)
    customer_names = np.where(is_org, org_names, person_full)

    # ------------------- Geography -------------------
    if region_name == "Europe":
        geo_region = geography[geography["Continent"] == "Europe"]
    else:
        geo_region = geography[geography["Country"] == region_name]

    selected_geo = geo_region.sample(
        n=count,
        replace=True,
        random_state=rng.integers(0, 1e9)
    )

    df = pd.DataFrame({
        "FullName": customer_names,
        "Gender": genders,
        "AddressLine1": "Address Placeholder",
        "AddressLine2": "",
        "City": selected_geo["City"].values,
        "StateProvince": selected_geo["State"].values,
        "Country": selected_geo["Country"].values,
        "PostalCode": rng.integers(10000, 999999, size=count),   # or "000000"
        "Region": region_name,
        "IsOrganization": is_org.astype(int)
    })

    return df


# ---------------------------------------------------------
# Main generator
# ---------------------------------------------------------

def generate_synthetic_customers(cfg, parquet_dims_folder):
    cust_cfg = cfg["customers"]
    rng = np.random.default_rng(cust_cfg["seed"])

    # Load geography dimension via loader
    geography, geo_changed = load_dimension(
        "geography",
        parquet_dims_folder,
        cfg["geography"]
    )

    total = cust_cfg["total_customers"]
    pct_org = cust_cfg.get("pct_org", 0.1)
    names_folder = Path("data/customer_names")

    # Distribution
    pct_india = cust_cfg.get("pct_india", 0.3)
    pct_us    = cust_cfg.get("pct_us", 0.4)
    pct_eu    = 1 - pct_india - pct_us

    count_india = int(total * pct_india)
    count_us    = int(total * pct_us)
    count_eu    = total - count_india - count_us

    # Generate
    customers = []

    with stage("Generating Customers"):
        customers.append(generate_region_customers("India", count_india, geography, names_folder, rng, pct_org))
        customers.append(generate_region_customers("United States", count_us, geography, names_folder, rng, pct_org))
        customers.append(generate_region_customers("Europe", count_eu, geography, names_folder, rng, pct_org))

        df = pd.concat(customers, ignore_index=True)

        # 🔥 Final deterministic unique keys
        df.insert(0, "CustomerKey", np.arange(1, len(df) + 1))

    return df


# ---------------------------------------------------------
# Pipeline entry
# ---------------------------------------------------------

def run_customers(cfg, parquet_folder: Path):
    out_path = parquet_folder / "customers.parquet"

    if not should_regenerate("customers", cfg, out_path):
        skip("Customers up-to-date; skipping.")
        return

    df = generate_synthetic_customers(cfg, parquet_folder)
    df.to_parquet(out_path, index=False)

    save_version("customers", cfg, out_path)
    info(f"Customers dimension written → {out_path}")
