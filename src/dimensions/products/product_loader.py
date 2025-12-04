import pandas as pd
from pathlib import Path
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from .contoso_loader import load_contoso_products
from .fake_generator import generate_fake_products


def load_product_dimension(config, output_folder: Path):
    p = config["products"]

    # Load upstream dims first (always from parquet_dims)
    df_cat = pd.read_parquet(output_folder / "product_category.parquet")
    df_sub = pd.read_parquet(output_folder / "product_subcategory.parquet")

    # Version key includes upstream actual data changes
    version_key = _version_key(p, df_cat, df_sub)

    # Versioning check
    if not should_regenerate(
        "products",
        version_key,
        output_folder / "products.parquet"
    ):
        skip("Products up-to-date; skipping regeneration")
        return _load_existing(output_folder)

    info("Loading Products")

    # Select mode: Contoso vs Fake
    if p["use_contoso_products"]:
        print("⚠️ USING CONTOSO PRODUCTS LOADER")
        df = load_contoso_products(output_folder)
    else:
        print("🔥 USING FAKE PRODUCT GENERATOR")
        df = generate_fake_products(p, output_folder)

    # Save version metadata
    save_version(
        "products",
        version_key,
        output_folder / "products.parquet"
    )

    return df


def _load_existing(folder: Path):
    return pd.read_parquet(folder / "products.parquet")


def _version_key(p, df_cat, df_sub):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_products": p["num_products"],

        # These guarantee regeneration whenever upstream dims change
        "num_categories_actual": len(df_cat),
        "num_subcategories_actual": len(df_sub),

        # Determinism for fake mode
        "seed": p.get("seed"),
    }
