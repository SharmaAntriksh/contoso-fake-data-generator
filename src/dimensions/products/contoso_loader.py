import pandas as pd
from pathlib import Path


def load_contoso_products(output_folder: Path):
    """
    Load Contoso products as-is but normalize only the required key columns:
    - ProductSubcategoryKey -> SubcategoryKey
    - ProductCategoryKey    -> CategoryKey

    All other Contoso columns are preserved untouched.
    """

    source_file = Path("data/contoso_products/products.parquet")
    df = pd.read_parquet(source_file)

    # Normalize only required keys
    rename_map = {}

    if "ProductSubcategoryKey" in df.columns:
        rename_map["ProductSubcategoryKey"] = "SubcategoryKey"

    if "ProductCategoryKey" in df.columns:
        rename_map["ProductCategoryKey"] = "CategoryKey"

    # ProductKey and cost/price are already correct in Contoso
    # No renaming needed

    df = df.rename(columns=rename_map)

    # Make sure required columns exist
    required = ["ProductKey", "SubcategoryKey", "UnitPrice", "UnitCost"]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Contoso products file missing required fields: {missing}"
        )

    # Save normalized (keys only) version
    df.to_parquet(output_folder / "products.parquet", index=False)

    return df
