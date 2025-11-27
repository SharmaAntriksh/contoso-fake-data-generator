import os
import pandas as pd
import numpy as np
from faker import Faker
import csv

# ============================================================
# LOAD NAME DATA
# ============================================================

def load_list(path):
    """Load names from a CSV file (single column)."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Missing file: {path}")

    s = pd.read_csv(path, header=None, dtype=str)[0].str.strip()
    s = s[s.str.match(r"^[A-Za-z\-\'. ]+$")]
    return s.str.title().unique()


# ============================================================
# LOAD REAL GEOGRAPHY
# ============================================================

def load_real_geography(config):
    """
    Load the final DimGeography (already built by geography_builder.py)
    and optionally apply continent/max_geos filters defined in config.
    """
    geo_cfg = config["customers"]["geography_source"]

    path = geo_cfg["path"]            # now points to FINAL geography.parquet
    continent = geo_cfg["continent"]
    max_geos = geo_cfg["max_geos"]

    if not os.path.isfile(path):
        raise FileNotFoundError(f"DimGeography not found: {path}")

    df_geo = pd.read_parquet(path)

    # Filtering by continent if configured
    if isinstance(continent, list):
        df_geo = df_geo[df_geo["Continent"].isin(continent)]
    elif continent != "All":
        df_geo = df_geo[df_geo["Continent"] == continent]

    if max_geos > 0:
        df_geo = df_geo.head(max_geos)

    return df_geo.reset_index(drop=True)



# ============================================================
# CUSTOMER GENERATOR WITH REAL GEOGRAPHY
# ============================================================

def generate_synthetic_customers(
    config,
    save_customer_csv=False,
    out_cust="SyntheticCustomers.csv"
):
    cust_cfg = config["customers"]

    total_customers = cust_cfg["total_customers"]
    pct_india = cust_cfg["pct_india"]
    pct_us = cust_cfg["pct_us"]
    pct_eu = cust_cfg["pct_eu"]
    pct_org = cust_cfg["pct_org"]
    seed = cust_cfg["seed"]
    names_folder = cust_cfg["names_folder"]

    rng = np.random.default_rng(seed)

    fake     = Faker()
    fake_in  = Faker("en_IN")
    fake_us  = Faker("en_US")
    fake_eu  = Faker("en_GB")

    # ============================================================
    # LOAD NAME DATASETS
    # ============================================================

    paths = {
        "us_male":     os.path.join(names_folder, "us_male_first.csv"),
        "us_female":   os.path.join(names_folder, "us_female_first.csv"),
        "us_last":     os.path.join(names_folder, "us_surnames.csv"),
        "in_first":    os.path.join(names_folder, "india_first.csv"),
        "in_last":     os.path.join(names_folder, "india_last.csv"),
        "eu_first":    os.path.join(names_folder, "eu_first.csv"),
        "eu_last":     os.path.join(names_folder, "eu_last.csv"),
    }

    us_male   = load_list(paths["us_male"])
    us_female = load_list(paths["us_female"])
    us_last   = load_list(paths["us_last"])
    in_first  = load_list(paths["in_first"])
    in_last   = load_list(paths["in_last"])
    eu_first  = load_list(paths["eu_first"])
    eu_last   = load_list(paths["eu_last"])

    # ============================================================
    # LOAD REAL GEOGRAPHY FROM PARQUET
    # ============================================================

    df_geo = load_real_geography(config)
    geo_keys = df_geo["GeographyKey"].to_numpy()

    # ============================================================
    # CUSTOMER GENERATION
    # ============================================================

    N = total_customers
    CustomerKey = np.arange(1, N + 1)

    Region = rng.choice(
        ["IN", "US", "EU"],
        size=N,
        p=[
            pct_india / 100,
            pct_us / 100,
            pct_eu / 100
        ]
    )

    IsOrg = rng.random(N) < (pct_org / 100)

    Gender = rng.choice(["M", "F"], size=N)
    Gender[IsOrg] = None

    # Geography from REAL DimGeography
    GeographyKey = rng.choice(geo_keys, size=N, replace=True)

    # Names
    FirstName = np.empty(N, dtype=object)
    LastName  = np.empty(N, dtype=object)

    # Last names
    mask = (Region == "IN") & (~IsOrg)
    LastName[mask] = rng.choice(in_last, size=mask.sum())

    mask = (Region == "US") & (~IsOrg)
    LastName[mask] = rng.choice(us_last, size=mask.sum())

    mask = (Region == "EU") & (~IsOrg)
    LastName[mask] = rng.choice(eu_last, size=mask.sum())

    LastName[IsOrg] = None

    # First names
    mask = (Region == "IN") & (~IsOrg)
    FirstName[mask] = rng.choice(in_first, size=mask.sum())

    mask = (Region == "US") & (~IsOrg) & (Gender == "M")
    FirstName[mask] = rng.choice(us_male, size=mask.sum())

    mask = (Region == "US") & (~IsOrg) & (Gender == "F")
    FirstName[mask] = rng.choice(us_female, size=mask.sum())

    mask = (Region == "EU") & (~IsOrg)
    FirstName[mask] = rng.choice(eu_first, size=mask.sum())

    FirstName[IsOrg] = None

    safe_first = np.where(FirstName == None, "", FirstName.astype(str))
    safe_last  = np.where(LastName  == None, "", LastName.astype(str))

    # ============================================================
    # ORGANIZATIONS
    # ============================================================

    company_pool = np.array([
        "TechNova", "BrightWave", "ZenithSystems", "PrimeSource",
        "ApexCorp", "GlobalWorks", "VertexInnovations",
        "OmniSoft", "NimbusSolutions", "SilverlineTech"
    ])

    CompanyName = np.empty(N, dtype=object)
    CompanyName[IsOrg] = company_pool[rng.integers(0, len(company_pool), size=IsOrg.sum())]
    CompanyName[~IsOrg] = None

    safe_company = np.where(CompanyName == None, "", CompanyName.astype(str))
    OrgDomain = np.where(IsOrg, np.char.lower(safe_company) + ".com", None)

    # ============================================================
    # EMAILS
    # ============================================================

    Email = np.empty(N, dtype=object)

    person_mask = ~IsOrg
    Email[person_mask] = (
        np.char.lower(safe_first[person_mask]) + "." +
        np.char.lower(safe_last[person_mask]) +
        rng.integers(10, 99999, size=person_mask.sum()).astype(str) +
        "@example.com"
    )

    Email[IsOrg] = "info@" + OrgDomain[IsOrg]

    # ============================================================
    # DISPLAY NAME
    # ============================================================

    CustomerName = np.where(
        IsOrg,
        "Organization " + CustomerKey.astype(str),
        safe_first + " " + safe_last
    )

    # ============================================================
    # DEMOGRAPHICS
    # ============================================================

    BirthDate = np.empty(N, dtype=object)
    if person_mask.sum():
        ages = rng.integers(18 * 365, 70 * 365, size=person_mask.sum())
        dates = pd.Timestamp("today").normalize() - pd.to_timedelta(ages, unit="D")
        BirthDate_vals = pd.to_datetime(dates).date
        BirthDate[person_mask] = BirthDate_vals

    BirthDate[IsOrg] = None

    MaritalStatus = np.where(IsOrg, None, rng.choice(["M", "S"], size=N))
    YearlyIncome = np.where(IsOrg, None, rng.integers(20000, 200000, size=N))

    TotalChildren = pd.Series(
        np.where(IsOrg, pd.NA, rng.integers(0, 5, size=N)),
        dtype="Int64"
    )

    Education = np.where(
        IsOrg,
        None,
        rng.choice(["High School", "Bachelors", "Masters", "PhD"],
                   size=N, p=[0.2, 0.5, 0.25, 0.05])
    )

    Occupation = np.where(
        IsOrg,
        None,
        rng.choice([
            "Professional", "Clerical", "Skilled",
            "Service", "Executive"
        ], size=N, p=[0.5, 0.2, 0.15, 0.1, 0.05])
    )

    # ============================================================
    # FINAL DF
    # ============================================================

    df = pd.DataFrame({
        "CustomerKey": CustomerKey,
        "CustomerName": CustomerName,
        "DOB": BirthDate,
        "MaritalStatus": MaritalStatus,
        "Gender": Gender,
        "EmailAddress": Email,
        "YearlyIncome": YearlyIncome,
        "TotalChildren": TotalChildren,
        "Education": Education,
        "Occupation": Occupation,
        "CustomerType": np.where(IsOrg, "Organization", "Person"),
        "CompanyName": CompanyName,
        "GeographyKey": GeographyKey,
    })

    if save_customer_csv:
        df.to_csv(out_cust, index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)

    return df
