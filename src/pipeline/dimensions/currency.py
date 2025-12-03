import pandas as pd


# Optional: Map ISO → readable currency names
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
    'currencies' is the ordered list from config:
        ["USD", "EUR", "INR", "GBP", ...]
    """
    iso = list(currencies)

    df = pd.DataFrame({
        "CurrencyKey": range(1, len(iso) + 1),
        "ISOCode": iso,
        "CurrencyName": [
            CURRENCY_NAME_MAP.get(c, c)  # fallback: use ISO as name
            for c in iso
        ],
    })

    return df
