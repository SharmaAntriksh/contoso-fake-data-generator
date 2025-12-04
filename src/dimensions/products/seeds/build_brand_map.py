# src/dimensions/products/seeds/build_brand_map.py

from src.dimensions.products.seeds.categories import CATEGORIES
from src.dimensions.products.seeds.category_brands import CATEGORY_BRANDS
from src.dimensions.products.seeds.subcategory_brands import SUBCATEGORY_BRANDS
from src.dimensions.products.seeds.fallback_brands import BRANDS


def build_brand_map():
    brand_map = {}

    # Priority 1: explicit subcategory brands
    for sub, brands in SUBCATEGORY_BRANDS.items():
        brand_map[sub] = brands

    # Priority 2: map remaining subcategories using category-level brands
    for category, subs in CATEGORIES.items():
        if category in CATEGORY_BRANDS:
            for sub in subs:
                if sub not in brand_map:
                    brand_map[sub] = CATEGORY_BRANDS[category]

    # Priority 3: fallback brands
    for category, subs in CATEGORIES.items():
        for sub in subs:
            if sub not in brand_map:
                brand_map[sub] = BRANDS

    return brand_map


BRAND_MAP = build_brand_map()
