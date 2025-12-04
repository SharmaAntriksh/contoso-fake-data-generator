# src/dimensions/products/category_loader.py
import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from src.dimensions.products.fake_product_seeds import CATEGORIES


def load_category_dimension(config, output_folder: Path):
    p = config["products"]
    version_key = _version_key(p)

    if not should_regenerate(
        "product_category",
        version_key,
        output_folder / "product_category.parquet"
    ):
        skip("Product Category up-to-date; skipping regeneration")
        return _load_existing(output_folder)

    info("Loading Product Category")

    if p["use_contoso_products"]:
        df = _load_contoso_category(output_folder)
    else:
        df = _generate_fake_category(p, output_folder)

    save_version(
        "product_category",
        version_key,
        output_folder / "product_category.parquet"
    )
    return df


# ---------------------------------------------------------
# CONTOSO MODE — Normalize Schema
# ---------------------------------------------------------
def _load_contoso_category(output_folder: Path):
    source_file = Path("data/contoso_products/product_category.parquet")
    df = pd.read_parquet(source_file)

    rename_map = {}

    # Contoso typically uses `Category`
    if "Category" in df.columns:
        rename_map["Category"] = "CategoryName"

    # Ensure key column is consistent
    if "CategoryKey" in df.columns:
        rename_map["CategoryKey"] = "ProductCategoryKey"

    df = df.rename(columns=rename_map)

    # Fail loudly if required column missing
    if "CategoryName" not in df.columns:
        raise ValueError(
            f"Missing CategoryName in Contoso category file. Columns: {df.columns}"
        )

    df.to_parquet(output_folder / "product_category.parquet", index=False)
    return df


# ---------------------------------------------------------
# FAKE MODE
# ---------------------------------------------------------
def _generate_fake_category(p, output_folder: Path):
    selected = list(CATEGORIES.keys())[: p["num_categories"]]

    df = pd.DataFrame({
        "ProductCategoryKey": range(1, len(selected) + 1),
        "CategoryName": selected
    })

    df.to_parquet(output_folder / "product_category.parquet", index=False)
    return df


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _load_existing(folder: Path):
    return pd.read_parquet(folder / "product_category.parquet")


def _version_key(p):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_categories": p["num_categories"],
        "seed": p.get("seed"),
    }
