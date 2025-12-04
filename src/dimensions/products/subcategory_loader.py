# src/dimensions/products/subcategory_loader.py
import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from src.dimensions.products.fake_product_seeds import CATEGORIES


def load_subcategory_dimension(config, output_folder: Path):
    p = config["products"]
    version_key = _version_key(p)

    if not should_regenerate(
        "product_subcategory",
        version_key,
        output_folder / "product_subcategory.parquet"
    ):
        skip("Product Subcategory up-to-date; skipping regeneration")
        return _load_existing(output_folder)

    info("Loading Product Subcategory")

    if p["use_contoso_products"]:
        df = _load_contoso_subcategory(output_folder)
    else:
        df = _generate_fake_subcategory(p, output_folder)

    save_version(
        "product_subcategory",
        version_key,
        output_folder / "product_subcategory.parquet"
    )
    return df


# ---------------------------------------------------------
# CONTOSO MODE — Normalize Schema
# ---------------------------------------------------------
def _load_contoso_subcategory(output_folder: Path):
    source_file = Path("data/contoso_products/product_subcategory.parquet")
    df = pd.read_parquet(source_file)

    rename_map = {}

    # Normalize subcategory name field
    if "Subcategory" in df.columns:
        rename_map["Subcategory"] = "SubcategoryName"
    if "Subcategory Label" in df.columns:
        rename_map["Subcategory Label"] = "SubcategoryName"

    # Normalize category key
    if "CategoryKey" in df.columns:
        rename_map["CategoryKey"] = "ProductCategoryKey"

    df = df.rename(columns=rename_map)

    # Ensure SubcategoryName exists
    if "SubcategoryName" not in df.columns:
        raise ValueError(
            f"Missing SubcategoryName in Contoso file. Columns: {df.columns}"
        )

    # Load category table to merge CategoryName
    df_cat = pd.read_parquet(output_folder / "product_category.parquet")

    df = df.merge(
        df_cat[["ProductCategoryKey", "CategoryName"]],
        on="ProductCategoryKey",
        how="left"
    )

    df.to_parquet(output_folder / "product_subcategory.parquet", index=False)
    return df


# ---------------------------------------------------------
# FAKE MODE
# ---------------------------------------------------------
def _generate_fake_subcategory(p, output_folder: Path):
    df_cat = pd.read_parquet(output_folder / "product_category.parquet")

    sub_rows = []
    current_id = 1

    for _, row in df_cat.iterrows():
        category_name = row["CategoryName"]
        category_key = row["ProductCategoryKey"]

        subs = CATEGORIES[category_name]
        max_subs = p.get("num_subcategories", len(subs))
        subs = subs[:max_subs]

        for sub_name in subs:
            sub_rows.append([
                current_id,
                category_key,
                category_name,
                sub_name
            ])
            current_id += 1

    df = pd.DataFrame(sub_rows, columns=[
        "ProductSubcategoryKey",
        "ProductCategoryKey",
        "CategoryName",
        "SubcategoryName"
    ])

    df.to_parquet(output_folder / "product_subcategory.parquet", index=False)
    return df


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _load_existing(folder: Path):
    return pd.read_parquet(folder / "product_subcategory.parquet")


def _version_key(p):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_categories": p.get("num_categories"),
        "num_subcategories": p.get("num_subcategories"),
        "seed": p.get("seed"),
    }
