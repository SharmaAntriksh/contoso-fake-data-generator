import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from src.dimensions.products.seeds.categories import CATEGORIES


def load_subcategory_dimension(config, output_folder: Path):
    p = config["products"]
    version_key = _version_key(p)
    parquet_path = output_folder / "product_subcategory.parquet"

    if not should_regenerate("product_subcategory", version_key, parquet_path):
        skip("Product Subcategory up-to-date; skipping regeneration")
        return pd.read_parquet(parquet_path)

    info("Loading Product Subcategory")

    if p["use_contoso_products"]:
        df = _load_contoso_subcategory(output_folder, parquet_path)
    else:
        df = _generate_fake_subcategory(config, output_folder, parquet_path)

    save_version("product_subcategory", version_key, parquet_path)
    return df


# ---------------------------------------------------------
# CONTOSO MODE
# ---------------------------------------------------------
def _load_contoso_subcategory(output_folder: Path, parquet_path: Path):
    src = Path("data/contoso_products/product_subcategory.parquet")
    df = pd.read_parquet(src)

    rename_map = {}

    # Normalize names
    if "ProductSubcategoryKey" in df.columns:
        rename_map["ProductSubcategoryKey"] = "SubcategoryKey"

    if "ProductCategoryKey" in df.columns:
        rename_map["ProductCategoryKey"] = "CategoryKey"

    if "SubcategoryName" in df.columns:
        rename_map["SubcategoryName"] = "Subcategory"
    elif "Subcategory Label" in df.columns:
        rename_map["Subcategory Label"] = "Subcategory"

    # CategoryName → Category
    if "CategoryName" in df.columns:
        rename_map["CategoryName"] = "Category"

    df = df.rename(columns=rename_map)

    # Ensure required columns
    required = ["SubcategoryKey", "CategoryKey", "Subcategory"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing required field {c} in Contoso subcategory file")

    # If Category missing, join with product_category.parquet
    if "Category" not in df.columns:
        cat_df = pd.read_parquet(output_folder / "product_category.parquet")
        df = df.merge(cat_df, on="CategoryKey", how="left")

    df = df[["SubcategoryKey", "CategoryKey", "Subcategory", "Category"]]

    df.to_parquet(parquet_path, index=False)
    return df


# ---------------------------------------------------------
# FAKE MODE
# ---------------------------------------------------------
def _generate_fake_subcategory(config, output_folder: Path, parquet_path: Path):
    df_cat = pd.read_parquet(output_folder / "product_category.parquet")
    p = config["products"]

    rows = []
    sub_key = 1

    for _, row in df_cat.iterrows():
        category_key = row["CategoryKey"]
        category_name = row["Category"]

        subs = CATEGORIES[category_name]

        for sub in subs:
            rows.append({
                "SubcategoryKey": sub_key,
                "CategoryKey": category_key,
                "Subcategory": sub,
                "Category": category_name   # ← IMPORTANT
            })
            sub_key += 1

    df = pd.DataFrame(rows)
    df.to_parquet(parquet_path, index=False)
    return df


# ---------------------------------------------------------
# Version key
# ---------------------------------------------------------
def _version_key(p):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_categories": p.get("num_categories"),
        "num_subcategories": p.get("num_subcategories"),
        "seed": p.get("seed"),
    }
