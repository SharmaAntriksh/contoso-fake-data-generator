import pandas as pd
from pathlib import Path


def load_contoso_products(output_folder: Path):
    """
    Loads the original Contoso products.parquet file from
    data/contoso_products and writes a copy into the dimension
    output folder (parquet_dims).
    """

    # Correct read-only Contoso source folder
    source_folder = Path("data/contoso_products")
    src_file = source_folder / "products.parquet"

    # Load original Contoso source file
    df = pd.read_parquet(src_file)

    # Write into parquet_dims output folder
    df.to_parquet(output_folder / "products.parquet", index=False)

    return df
