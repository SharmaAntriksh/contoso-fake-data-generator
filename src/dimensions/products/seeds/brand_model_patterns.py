# src/dimensions/products/seeds/brand_model_patterns.py

import random

BRAND_MODEL_PATTERNS = {
    "Sony": lambda: f"SN-{random.randint(100,9999)}",
    "Samsung": lambda: f"SM-{random.randint(100,9999)}",
    "LG": lambda: f"LG-{random.randint(100,9999)}",
    "Apple": lambda: f"A{random.randint(1000,9999)}",
    "Dell": lambda: f"D-{random.randint(1000,9999)}",
    "HP": lambda: f"HP-{random.randint(1000,9999)}",

    "Canon": lambda: f"C-{random.randint(100,9999)}",
    "Nikon": lambda: f"N-{random.randint(100,9999)}",
    "GoPro": lambda: f"GP-{random.randint(100,9999)}",

    "Xbox": lambda: f"XBX-{random.randint(100,9999)}",
    "Nintendo": lambda: f"NTD-{random.randint(100,9999)}",

    "_default": lambda: f"MDL-{random.randint(1000,9999)}",
}
