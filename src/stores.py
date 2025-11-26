import pandas as pd
import numpy as np


def generate_store_table(
    geography_parquet_path,
    num_stores=200,
    opening_start="2018-01-01",
    opening_end="2023-01-31",
    closing_end="2025-12-31",
    seed=42
):
    """
    Generate synthetic store dimension table.
    Output structure and logic match original exactly.
    """
    np.random.seed(seed)

    # --------------------------------------------------------
    # Load geography
    # --------------------------------------------------------
    geo = pd.read_parquet(geography_parquet_path)
    if "GeographyKey" not in geo.columns:
        raise ValueError("Parquet must contain GeographyKey column.")

    geo_keys = geo["GeographyKey"].tolist()

    # --------------------------------------------------------
    # Lookup values
    # --------------------------------------------------------
    store_types = ["Supermarket", "Convenience", "Online", "Hypermarket"]
    store_status = ["Open", "Closed", "Renovating"]
    close_reasons = ["Low Sales", "Lease Ended", "Renovation", "Moved Location"]

    # --------------------------------------------------------
    # Base table
    # --------------------------------------------------------
    df = pd.DataFrame({"StoreKey": range(1, num_stores + 1)})

    df["StoreName"] = df["StoreKey"].apply(lambda x: f"Store #{x:04d}")
    df["StoreType"] = np.random.choice(
        store_types, num_stores, p=[0.5, 0.3, 0.1, 0.1]
    )
    df["Status"] = np.random.choice(
        store_status, num_stores, p=[0.85, 0.10, 0.05]
    )

    # Geography assignment
    df["GeographyKey"] = np.random.choice(geo_keys, num_stores)

    # --------------------------------------------------------
    # Opening dates
    # --------------------------------------------------------
    open_start_ts = pd.Timestamp(opening_start).value // 10**9
    open_end_ts   = pd.Timestamp(opening_end).value   // 10**9

    df["OpeningDate"] = pd.to_datetime(
        np.random.randint(open_start_ts, open_end_ts, num_stores),
        unit="s"
    )

    # --------------------------------------------------------
    # Closing dates (only for closed stores)
    # --------------------------------------------------------
    closing_ts = pd.Timestamp(closing_end).value // 10**9

    def generate_close_date(row):
        if row["Status"] != "Closed":
            return pd.NaT
        # closing can only happen after opening
        open_ts = row["OpeningDate"].value // 10**9
        return pd.to_datetime(
            np.random.randint(open_ts, closing_ts), unit="s"
        )

    df["ClosingDate"] = df.apply(generate_close_date, axis=1)

    # Flag for "Open"
    df["OpenFlag"] = (df["Status"] == "Open").astype(int)

    # --------------------------------------------------------
    # Basic store attributes
    # --------------------------------------------------------
    df["SquareFootage"] = np.random.randint(2000, 10000, num_stores)
    df["EmployeeCount"] = np.random.randint(10, 120, num_stores)
    df["StoreManager"] = df["StoreKey"].apply(lambda x: f"Manager {x:04d}")
    df["Phone"] = df["StoreKey"].apply(
        lambda x: f"(555) {x % 900 + 100}-{x % 10000:04d}"
    )

    # --------------------------------------------------------
    # Descriptions & CloseReason
    # --------------------------------------------------------
    df["StoreDescription"] = (
        df["StoreType"] + " located in GeographyKey " + df["GeographyKey"].astype(str)
    )

    df["CloseReason"] = df.apply(
        lambda r: np.random.choice(close_reasons) if r["Status"] == "Closed" else "",
        axis=1
    )

    return df
