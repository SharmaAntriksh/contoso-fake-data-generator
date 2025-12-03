import pandas as pd
import numpy as np
from pathlib import Path
import yfinance as yf
from datetime import timedelta

from src.utils.logging_utils import info

# ---------------------------------------------------------
# DEFAULT CURRENCY LIST
# ---------------------------------------------------------
CURRENCIES = [
    "EUR", "GBP", "INR", "AUD", "CAD", "CNY",
    "JPY", "SGD", "CHF", "ZAR", "HKD", "NZD", "SEK"
]

BASE = "USD"


# ---------------------------------------------------------
# Download single currency history
# ---------------------------------------------------------
def download_history(currency, start_date, end_date):
    """
    Download historical FX data for currency/USD from Yahoo Finance.
    Ensures Date is always datetime64[ns], and columns are flat.
    """
    if currency == BASE:
        dates = pd.date_range(start=start_date, end=end_date)
        return pd.DataFrame({"Date": dates, "Rate": 1.0})

    # INR uses USDINR=X (invert)
    if currency == "INR":
        ticker = "USDINR=X"
        invert = False
    else:
        ticker = f"{currency}{BASE}=X"
        invert = False

    data = yf.download(
        ticker,
        start=start_date - timedelta(days=3),
        end=end_date + timedelta(days=3),
        auto_adjust=False,
        progress=False
    )

    if data.empty:
        raise ValueError(f"No FX data found for {currency}")

    # FIX: flatten MultiIndex columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ['_'.join([str(c) for c in col if c]).strip()
                        for col in data.columns.values]

    # Prefer "Close" or "Close_" depending on Yahoo’s structure
    rate_col = None
    for col in data.columns:
        if col.lower().startswith("close"):
            rate_col = col
            break

    if rate_col is None:
        raise ValueError(f"No 'Close' column found for {currency}. Found: {data.columns}")

    data = data[[rate_col]].rename(columns={rate_col: "Rate"})
    data = data.reset_index().rename(columns={"Date": "Date"})

    # Convert Date to datetime64
    data["Date"] = pd.to_datetime(data["Date"])

    if invert:
        data["Rate"] = 1 / data["Rate"]

    return data


# ---------------------------------------------------------
# Fill missing days safely
# ---------------------------------------------------------
def fill_missing_days(df, start_date, end_date):
    """
    Fill weekends/holidays with forward fill.
    Ensures both dfs use datetime64 for Date.
    """
    # Ensure datetime
    df["Date"] = pd.to_datetime(df["Date"])

    full = pd.DataFrame({"Date": pd.date_range(start=start_date, end=end_date)})

    merged = full.merge(df, on="Date", how="left")

    merged["Rate"] = merged["Rate"].ffill()
    merged["Rate"] = merged["Rate"].bfill().fillna(1.0)

    return merged


# ---------------------------------------------------------
# Build or update master FX store
# ---------------------------------------------------------
def build_or_update_fx(start_date, end_date, out_path, currencies=None):
    """
    Build or update a master FX file covering the date range.

    Ensures consistent datetime64 Date column.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    curr_list = currencies or CURRENCIES

    # -------------------------------------------
    # Load existing master file (keep datetime)
    # -------------------------------------------
    if out_path.exists():
        master = pd.read_parquet(out_path)
        # FIX: ensure dates are datetime.date
        master["Date"] = pd.to_datetime(master["Date"]).dt.date
    else:
        master = pd.DataFrame(columns=["Date", "FromCurrency", "ToCurrency", "Rate"])

    # FIX: normalize input types
    if isinstance(start_date, pd.Timestamp):
        start_date = start_date.date()
    if isinstance(end_date, pd.Timestamp):
        end_date = end_date.date()

    # -------------------------------------------
    # Determine missing ranges
    # -------------------------------------------
    if master.empty:
        existing_start = None
        existing_end = None
    else:
        existing_start = master["Date"].min()
        existing_end = master["Date"].max()

    need_download_start = existing_start is None or start_date < existing_start
    need_download_end   = existing_end is None or end_date > existing_end

    updates = []

    # -------------------------------------------
    # Download updates per currency
    # -------------------------------------------
    for cur in curr_list:
        info(f"Updating FX for {cur}...")

        if master.empty:
            cur_existing_start = None
            cur_existing_end = None
        else:
            df_cur = master[master["ToCurrency"] == cur]
            if df_cur.empty:
                cur_existing_start = None
                cur_existing_end = None
            else:
                cur_existing_start = df_cur["Date"].min()
                cur_existing_end = df_cur["Date"].max()

        # Missing windows
        dl_start = start_date if (cur_existing_start is None or start_date < cur_existing_start) else cur_existing_start
        dl_end   = end_date   if (cur_existing_end is None   or end_date > cur_existing_end)   else cur_existing_end

        # Download + ensure datetime
        df_dl = download_history(cur, dl_start, dl_end)

        # Fill missing days
        df_dl = fill_missing_days(df_dl, dl_start, dl_end)

        df_dl["FromCurrency"] = BASE
        df_dl["ToCurrency"] = cur

        updates.append(df_dl)

    # -------------------------------------------
    # Combine master + updates
    # -------------------------------------------
    updates_df = pd.concat(updates, ignore_index=True)

    if master.empty:
        master_updated = updates_df
    else:
        master_updated = pd.concat([master, updates_df], ignore_index=True)
        master_updated["Date"] = pd.to_datetime(master_updated["Date"])
        master_updated = (
            master_updated
            .drop_duplicates(subset=["Date", "FromCurrency", "ToCurrency"], keep="last")
            .sort_values("Date")
        )

    # -------------------------------------------
    # Save
    # -------------------------------------------
    master_updated.to_parquet(out_path, index=False)

    return master_updated
