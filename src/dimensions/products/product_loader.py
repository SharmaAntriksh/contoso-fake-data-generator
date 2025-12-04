from pathlib import Path
import pandas as pd
from src.utils import info, skip
from src.versioning import should_regenerate, save_version

from .contoso_loader import load_contoso_products
from .fake_generator import generate_fake_products


def load_product_dimension(config, output_folder: Path):
    p = config["products"]
    version_key = _version_key(p)
    parquet_path = output_folder / "products.parquet"

    # If unchanged, skip
    if not should_regenerate("products", version_key, parquet_path):
        skip("Products up-to-date; skipping regeneration")
        return pd.read_parquet(parquet_path)

    # Mode
    if p["use_contoso_products"]:
        info("📦 USING CONTOSO PRODUCTS")
        df = load_contoso_products(output_folder)
    else:
        info("🔥 USING FAKE PRODUCT GENERATOR")
        generated_path = generate_fake_products(p, output_folder)
        df = pd.read_parquet(generated_path)

    # Required minimal fields for the Sales fact pipeline
    required = ["ProductKey", "SubcategoryKey", "UnitPrice", "UnitCost"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required field in Products: {col}")

    # Write final parquet
    df.to_parquet(parquet_path, index=False)

    # Save versioning
    save_version("products", version_key, parquet_path)

    return df


# ---------------------------------------------------------
# Version key consistent with your design
# ---------------------------------------------------------
def _version_key(p):
    return {
        "use_contoso_products": p["use_contoso_products"],
        "num_products": p["num_products"],
        "seed": p.get("seed"),
    }
