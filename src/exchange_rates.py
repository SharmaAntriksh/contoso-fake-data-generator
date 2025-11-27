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
    rng = np.random.default_rng(seed)

    dates = pd.date_range(start_date, end_date, freq="D")

    # Starting seed values (fallback if new currencies added)
    base_rates = {
        "USD": 1.0,
        "EUR": 0.90,
        "INR": 83.0,
        "GBP": 0.78
    }
    for c in currencies:
        base_rates.setdefault(c, rng.uniform(0.5, 100.0))

    rows = []

    # =============================================================
    # 1) BASE → BASE (e.g., USD → USD)
    # =============================================================
    for d in dates:
        rows.append({
            "Date": d.date(),
            "FromCurrency": base_currency,
            "ToCurrency": base_currency,
            "ExchangeRate": 1.000000
        })

    # =============================================================
    # 2) BASE → OTHER CURRENCIES
    # =============================================================
    for c in currencies:
        if c == base_currency:
            continue

        rate = base_rates[c]

        for d in dates:
            shock = rng.normal(0, volatility)
            rate *= (1 + shock)
            rate = max(rate, 0.0001)

            rows.append({
                "Date": d.date(),
                "FromCurrency": base_currency,
                "ToCurrency": c,
                "ExchangeRate": round(rate, 6)
            })

    return pd.DataFrame(rows)
