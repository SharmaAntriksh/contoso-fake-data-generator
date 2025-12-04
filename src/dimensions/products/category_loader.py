import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from .fake_product_seeds import CATEGORIES   # dict: category → subcategories


def load_category_dimension(config, output_folder: Path):
    p = config["products"]
    version_key = _version_key(p)

    # Check if we need to regenerate category table
    if not should_regenerate(
        "product_category",
        version_key,
        output_folder / "product_category.parquet"
    ):
        skip("Product Category up-to-date; skipping regeneration")
        return _load_existing(output_folder)

    info("Loading Product Category")

    # Contoso vs Fake selection
    if p["use_contoso_products"]:
        df = _load_contoso_category(output_folder)
    else:
        df = _generate_fake_category(p, output_folder)

    # Save version metadata
    save_version(
        "product_category",
        version_key,
        output_folder / "product_category.parquet"
    )
    return df


# ---------------------------------------------------------------------
# Contoso Category Loader  (READ-ONLY SOURCE)
# ---------------------------------------------------------------------
def _load_contoso_category(output_folder: Path):
    # Original Contoso files are stored here (never overwritten)
    source_file = Path("data/contoso_products/product_category.parquet")

    df = pd.read_parquet(source_file)

    # Write to output folder (parquet_dims)
    df.to_parquet(output_folder / "product_category.parquet", index=False)
    return df


# ---------------------------------------------------------------------
# Fake Category Generator
# ---------------------------------------------------------------------
def _generate_fake_category(p, output_folder: Path):
    num_categories = p["num_categories"]

    # Pick N categories from seed list
    selected = list(CATEGORIES.keys())[:num_categories]

    df = pd.DataFrame({
        "ProductCategoryKey": range(1, len(selected) + 1),
        "CategoryName": selected,
    })

    # Write output
    df.to_parquet(output_folder / "product_category.parquet", index=False)
    return df


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _load_existing(folder: Path):
    return pd.read_parquet(folder / "product_category.parquet")


def _version_key(p):
    # Version key ensures regeneration when:
    # - switching between Contoso / Fake
    # - changing num_categories
    # - changing seed
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_categories": p["num_categories"],
        "seed": p.get("seed"),
    }
