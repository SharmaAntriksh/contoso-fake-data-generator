import pandas as pd
import numpy as np


def generate_exchange_rate_table(
    start_date,
    end_date,
    currencies,
    base_currency="USD",
    volatility=0.02,
    seed=42
):
    """
    Generate daily FX rates using a simple geometric random walk model.
    Always produces:
        - Base → Base rate (1.0)
        - Base → Other rates

    Parameters
    ----------
    start_date : str
    end_date   : str
    currencies : list of ISO currency codes (["USD", "EUR", ...])
    base_currency : str
    volatility : float  (daily std deviation)
    seed : int
    """

    rng = np.random.default_rng(seed)
    dates = pd.date_range(start_date, end_date, freq="D")

    # ------------------------------------------------------------
    # Realistic USD → X starting rates (fallback included)
    # ------------------------------------------------------------
    base_rates = {
        "USD": 1.00,
        "EUR": 0.90,
        "INR": 83.00,
        "GBP": 0.78,
        "AUD": 1.45,
        "CAD": 1.34,
        "CNY": 6.80,
        "JPY": 110.0,
        "SGD": 1.35,
        "DKK": 6.80,
        "SEK": 9.50,
        "CHF": 0.92,
        "NZD": 1.55,
        "ZAR": 18.0,
    }

    # Add fallback for any missing currency
    for c in currencies:
        if c not in base_rates:
            base_rates[c] = float(rng.uniform(0.5, 2.0))

    rows = []

    # ------------------------------------------------------------
    # BASE → BASE (straight line = 1.0)
    # ------------------------------------------------------------
    for d in dates:
        rows.append({
            "Date": d.date(),
            "FromCurrency": base_currency,
            "ToCurrency": base_currency,
            "ExchangeRate": 1.0
        })

    # ------------------------------------------------------------
    # BASE → OTHER (random walk)
    # ------------------------------------------------------------
    for c in currencies:
        if c == base_currency:
            continue

        rate = float(base_rates[c])

        for d in dates:
            shock = rng.normal(0, volatility)
            rate *= (1 + shock)
            rate = max(rate, 0.0001)  # avoid zeros

            rows.append({
                "Date": d.date(),
                "FromCurrency": base_currency,
                "ToCurrency": c,
                "ExchangeRate": round(rate, 6),
            })

    return pd.DataFrame(rows)
