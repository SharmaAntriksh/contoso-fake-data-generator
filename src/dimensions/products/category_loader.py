import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from src.dimensions.products.seeds.categories import CATEGORIES


def load_category_dimension(config, output_folder: Path):
    p = config["products"]
    version_key = _version_key(p)
    parquet_path = output_folder / "product_category.parquet"

    # Skip if already up-to-date
    if not should_regenerate("product_category", version_key, parquet_path):
        skip("Product Category up-to-date; skipping regeneration")
        return pd.read_parquet(parquet_path)

    info("Loading Product Category")

    if p["use_contoso_products"]:
        df = _load_contoso_category(parquet_path)
    else:
        df = _generate_fake_category(p, parquet_path)

    save_version("product_category", version_key, parquet_path)
    return df


# ---------------------------------------------------------
# CONTOSO MODE — Normalize to (CategoryKey, Category)
# ---------------------------------------------------------
def _load_contoso_category(parquet_path: Path):
    source_file = Path("data/contoso_products/product_category.parquet")
    df = pd.read_parquet(source_file)

    rename_map = {}

    # Normalize names
    if "CategoryName" in df.columns:
        rename_map["CategoryName"] = "Category"

    if "ProductCategoryKey" in df.columns:
        rename_map["ProductCategoryKey"] = "CategoryKey"

    # Apply renaming
    df = df.rename(columns=rename_map)

    # Validate required fields
    required = ["CategoryKey", "Category"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Contoso category file missing required fields: {missing}")

    # Keep clean output
    df = df[["CategoryKey", "Category"]]

    df.to_parquet(parquet_path, index=False)
    return df


# ---------------------------------------------------------
# FAKE MODE — Uses taxonomy seed list
# ---------------------------------------------------------
def _generate_fake_category(p, parquet_path: Path):
    selected_cats = list(CATEGORIES.keys())[: p["num_categories"]]

    df = pd.DataFrame({
        "CategoryKey": range(1, len(selected_cats) + 1),
        "Category": selected_cats,
    })

    df.to_parquet(parquet_path, index=False)
    return df


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _version_key(p):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_categories": p["num_categories"],
        "seed": p.get("seed"),
    }
