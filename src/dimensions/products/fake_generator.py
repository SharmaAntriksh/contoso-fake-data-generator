import random
import numpy as np
import pandas as pd
from pathlib import Path

from .fake_product_seeds import (
    BASE_PRODUCT_NAMES,
    ADJECTIVES,
    SERIES
)


def generate_fake_products(p, output_folder: Path):
    """
    Generates fake product rows based on chosen subcategories.
    Output schema matches Contoso expectations so Sales can run.

    Columns generated:
    - ProductKey
    - ProductSubcategoryKey
    - ProductName
    - UnitPrice
    - UnitCost
    """

    print("🔥 USING FAKE PRODUCT GENERATOR")

    # Seed ensures reproducibility
    random.seed(p.get("seed", 42))
    rng = np.random.default_rng(p.get("seed", 42))

    num_products = p["num_products"]

    # Load subcategories already generated/copied
    df_sub = pd.read_parquet(output_folder / "product_subcategory.parquet")

    product_rows = []

    for product_key in range(1, num_products + 1):
        # Random subcategory row
        sub = df_sub.sample(1).iloc[0]

        sub_key = sub["ProductSubcategoryKey"]
        sub_name = sub["SubcategoryName"]

        # Pick name components
        base_names = BASE_PRODUCT_NAMES[sub_name]
        base = random.choice(base_names)
        adj = random.choice(ADJECTIVES)
        series = random.choice(SERIES)
        number = random.randint(10, 9999)

        # Construct product name
        product_name = f"{adj} {base} {series} {number}"

        # -----------------------------
        # PRICE GENERATION (required!)
        # -----------------------------

        # Category-based price ranges (approx. Contoso values)
        PRICE_RANGES = {
            "Audio": (50, 400),
            "TV & Video": (150, 1500),
            "Computers": (200, 2500),
            "Cameras and camcorders": (80, 800),
            "Cell phones": (100, 1200),
            "Music, Movies and Audio Books": (5, 50),
            "Games and Toys": (10, 200),
            "Home Appliances": (40, 600),
        }

        # Get category to determine price range
        try:
            cat_range = PRICE_RANGES[sub["CategoryName"]]
        except:
            # fallback range
            cat_range = (20, 500)

        # Price generation
        unit_price = float(rng.uniform(*cat_range))
        unit_price = round(unit_price, 2)

        # Cost is always lower
        unit_cost = round(unit_price * rng.uniform(0.5, 0.9), 2)

        # Append row
        product_rows.append([
            product_key,
            sub_key,
            product_name,
            unit_price,
            unit_cost
        ])

    # Final DataFrame
    df_products = pd.DataFrame(
        product_rows,
        columns=[
            "ProductKey",
            "ProductSubcategoryKey",
            "ProductName",
            "UnitPrice",
            "UnitCost"
        ]
    )

    # Save output
    df_products.to_parquet(output_folder / "products.parquet", index=False)

    return df_products
