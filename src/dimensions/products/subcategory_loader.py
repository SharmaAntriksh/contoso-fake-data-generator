import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from .fake_product_seeds import CATEGORIES   # category → subcategories


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


# ---------------------------------------------------------------------
# CONTOSO MODE  (READ-ONLY SOURCE)
# ---------------------------------------------------------------------
def _load_contoso_subcategory(output_folder: Path):
    # Load original Contoso subcategory file
    source_file = Path("data/contoso_products/product_subcategory.parquet")
    df = pd.read_parquet(source_file)

    # Write to output folder (parquet_dims)
    df.to_parquet(output_folder / "product_subcategory.parquet", index=False)
    return df


# ---------------------------------------------------------------------
# FAKE MODE
# ---------------------------------------------------------------------
def _generate_fake_subcategory(p, output_folder: Path):
    # Always load the *generated* or *copied* categories from output folder
    df_cat = pd.read_parquet(output_folder / "product_category.parquet")

    sub_rows = []
    current_id = 1

    for _, row in df_cat.iterrows():
        category_name = row["CategoryName"]
        category_key = row["ProductCategoryKey"]

        # Seed subcategories for this category
        subs = CATEGORIES[category_name]

        # Limit subcategories if user defined a max
        max_subs = p.get("num_subcategories", len(subs))
        subs = subs[:max_subs] if max_subs < len(subs) else subs

        for sub_name in subs:
            sub_rows.append([
                current_id,
                category_key,
                sub_name
            ])
            current_id += 1

    df = pd.DataFrame(sub_rows, columns=[
        "ProductSubcategoryKey",
        "ProductCategoryKey",
        "SubcategoryName"
    ])

    # Save generated subcategories
    df.to_parquet(output_folder / "product_subcategory.parquet", index=False)
    return df


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _load_existing(folder: Path):
    return pd.read_parquet(folder / "product_subcategory.parquet")


def _version_key(p):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_categories": p.get("num_categories"),
        "num_subcategories": p.get("num_subcategories"),
        "seed": p.get("seed"),
    }
