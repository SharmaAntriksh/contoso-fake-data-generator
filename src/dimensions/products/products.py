from src.utils import info
from pathlib import Path

from .category_loader import load_category_dimension
from .subcategory_loader import load_subcategory_dimension
from .product_loader import load_product_dimension


def generate_product_dimension(config, output_folder: Path):
    """
    Orchestrates the full Product dimension:
    1. Product Category
    2. Product Subcategory
    3. Product

    Ensures ordering and proper regeneration.
    """

    info("Starting Product Dimension")

    df_cat = load_category_dimension(config, output_folder)
    df_sub = load_subcategory_dimension(config, output_folder)
    df_prod = load_product_dimension(config, output_folder)

    return {
        "category": df_cat,
        "subcategory": df_sub,
        "product": df_prod
    }

