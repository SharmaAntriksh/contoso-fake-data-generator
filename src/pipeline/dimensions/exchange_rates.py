import pandas as pd

def generate_exchange_rate_table(
    start_date,
    end_date,
    currencies,
    base_currency,
    master_path
):
    """
    Robust slicer for the master exchange_rates parquet.
    Ensures Date is normalized, To/FromCurrency exist, and returns the requested slice.
    """

    # read
    df = pd.read_parquet(master_path)

    # basic sanity checks & normalization
    if "Date" not in df.columns:
        raise RuntimeError(f"master file {master_path} missing Date column")
    if "FromCurrency" not in df.columns or "ToCurrency" not in df.columns:
        raise RuntimeError(f"master file {master_path} missing currency columns")

    # normalize Date column to pandas datetime (ensures no Timestamp/date mix)
    df["Date"] = pd.to_datetime(df["Date"])

    # Normalize string columns (strip, upper) — helps avoid accidental mismatches
    df["FromCurrency"] = df["FromCurrency"].astype(str).str.strip().str.upper()
    df["ToCurrency"]   = df["ToCurrency"].astype(str).str.strip().str.upper()

    # Normalize inputs
    start = pd.to_datetime(start_date)
    end   = pd.to_datetime(end_date)
    base_currency = str(base_currency).upper()
    currencies = [str(c).upper() for c in currencies]

    # Build mask
    mask = (
        (df["Date"] >= start) &
        (df["Date"] <= end) &
        (df["FromCurrency"] == base_currency) &
        (df["ToCurrency"].isin(currencies))
    )

    result = df.loc[mask].copy()

    # If empty, give a helpful diagnostic sample
    if result.empty:
        # capture small samples to help debugging
        sample_counts = df["ToCurrency"].value_counts().head(10).to_dict()
        raise RuntimeError(
            "Exchange rate slice is empty. Diagnostics:\n"
            f"master total rows={len(df)}, sample ToCurrency counts={sample_counts}\n"
            f"Requested base_currency={base_currency}, currencies={currencies}, start={start_date}, end={end_date}\n"
            f"Master path: {master_path}"
        )

    # ensure ExchangeRate numeric and rounded to 6 decimals
    result["ExchangeRate"] = pd.to_numeric(result["ExchangeRate"], errors="coerce").round(6)

    # final sort
    result.sort_values(["Date", "ToCurrency"], inplace=True)
    result.reset_index(drop=True, inplace=True)
    return result
