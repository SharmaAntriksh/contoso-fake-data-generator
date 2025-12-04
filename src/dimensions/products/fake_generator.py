# src/dimensions/products/fake_generator.py

import random
import numpy as np
import pandas as pd
from pathlib import Path

# Unified seed imports
from src.dimensions.products.fake_product_seeds import (
    BASE_PRODUCT_NAMES,
    ADJECTIVES,
    SERIES,
    BRANDS,
    BRAND_MAP,
    CATEGORY_BRANDS,
    PRICE_RANGES,
    BRAND_CLASS,
    CATEGORY_COLORS,
    CATEGORY_STOCK_TYPE,
)


def generate_fake_products(p, output_folder: Path):

    random.seed(p.get("seed", 42))
    rng = np.random.default_rng(p.get("seed", 42))

    num_products = p["num_products"]

    # Load subcategory table
    df_sub = pd.read_parquet(output_folder / "product_subcategory.parquet")
    
    # Ensure expected columns exist after loader split
    # Expected: ProductSubcategoryKey, Subcategory, CategoryName
    if "Subcategory" in df_sub.columns:
        df_sub = df_sub.rename(columns={"Subcategory": "SubcategoryName"})
    if "Category" in df_sub.columns:
        df_sub = df_sub.rename(columns={"Category": "CategoryName"})

    rows = []

    for product_key in range(1, num_products + 1):

        sub = df_sub.sample(1).iloc[0]

        sub_key = sub["ProductSubcategoryKey"]
        sub_name = sub["SubcategoryName"]
        category_name = sub["CategoryName"]

        # -------- PRODUCT NAME --------
        base = random.choice(BASE_PRODUCT_NAMES[sub_name])
        adj = random.choice(ADJECTIVES)
        series = random.choice(SERIES)
        number = random.randint(10, 9999)

        product_name = f"{adj} {base} {series} {number}"

        # -------- DESCRIPTION --------
        product_description = (
            f"A high quality {base.lower()} designed for everyday use."
        )

        # -------- PRODUCT CODE --------
        product_code = f"{product_key:07d}"

        # -------- PRICING --------
        cat_range = PRICE_RANGES.get(category_name, (20, 500))
        unit_price = round(float(rng.uniform(*cat_range)), 2)
        unit_cost = round(unit_price * float(rng.uniform(0.5, 0.9)), 2)

        # -------- BRAND --------
        if sub_name in BRAND_MAP:
            brand = random.choice(BRAND_MAP[sub_name])
        elif category_name in CATEGORY_BRANDS:
            brand = random.choice(CATEGORY_BRANDS[category_name])
        else:
            brand = random.choice(BRANDS)

        # -------- CLASS --------
        product_class = BRAND_CLASS.get(brand, BRAND_CLASS["_default"])

        # -------- COLOR --------
        color_choices = CATEGORY_COLORS.get(category_name, ["Black"])
        color = random.choice(color_choices)

        # -------- STOCK TYPE --------
        stock_options = CATEGORY_STOCK_TYPE.get(category_name, ["High"])
        stock_type = random.choice(stock_options)
        stock_code = str(stock_options.index(stock_type) + 1)

        rows.append([
            product_key,
            product_code,
            product_name,
            product_description,
            sub_key,
            brand,
            product_class,
            color,
            stock_code,
            stock_type,
            unit_cost,
            unit_price
        ])

    df = pd.DataFrame(rows, columns=[
        "ProductKey",
        "ProductCode",
        "ProductName",
        "ProductDescription",
        "ProductSubcategoryKey",
        "Brand",
        "Class",
        "Color",
        "StockTypeCode",
        "StockType",
        "UnitCost",
        "UnitPrice"
    ])

    df.to_parquet(output_folder / "products.parquet", index=False)
    return df
