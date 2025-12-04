# src/dimensions/products/seeds/util_seeds.py

import random
import datetime as dt
from src.dimensions.products.seeds.brand_model_patterns import BRAND_MODEL_PATTERNS


def generate_ean13():
    """Generate a valid EAN-13 barcode."""
    digits = [random.randint(0, 9) for _ in range(12)]
    checksum = (sum(digits[::2]) + sum([d * 3 for d in digits[1::2]]))
    checksum = (10 - (checksum % 10)) % 10
    digits.append(checksum)
    return ''.join(map(str, digits))


def make_release_dates(rng):
    """Generate realistic release + discontinued dates."""
    release = dt.date(
        rng.integers(2015, 2024),
        rng.integers(1, 12),
        rng.integers(1, 28)
    )

    discontinued = None
    if rng.random() < 0.2:  # 20% discontinued
        discontinued = release + dt.timedelta(
            days=int(rng.integers(180, 2000))
        )

    return release, discontinued


def model_code_for(brand: str) -> str:
    """Pick model number pattern based on brand."""
    fn = BRAND_MODEL_PATTERNS.get(brand, BRAND_MODEL_PATTERNS["_default"])
    return fn()
