# src/dimensions/products/fake_product_seeds.py
"""
Unified access point for all product seed definitions.
This keeps backwards compatibility with older imports while
splitting the actual seed logic into smaller modular files.
"""

# -------------------------------
# Taxonomy (category → subcategory → names → brands)
# -------------------------------
from src.dimensions.products.seeds.taxonomy_seeds import (
    CATEGORIES,
    BASE_PRODUCT_NAMES,
    CATEGORY_BRANDS,
    SUBCATEGORY_BRANDS,
    BRANDS,
    BRAND_MAP,
)

# -------------------------------
# Attributes (colors, pricing, weights, descriptions, class, etc.)
# -------------------------------
from src.dimensions.products.seeds.attribute_seeds import (
    CATEGORY_COLORS,
    CATEGORY_DESCRIPTIONS,
    BRAND_CLASS,
    CATEGORY_WEIGHT,
    PRICE_RANGES,
    CATEGORY_STOCK_TYPE,
    ADJECTIVES,
    SERIES,
)

# -------------------------------
# Utility functions (EAN, model numbers, release dates)
# -------------------------------
from src.dimensions.products.seeds.util_seeds import (
    generate_ean13,
    make_release_dates,
    model_code_for,
)

# -------------------------------
# Model number patterns
# -------------------------------
from src.dimensions.products.seeds.brand_model_patterns import (
    BRAND_MODEL_PATTERNS,
)
