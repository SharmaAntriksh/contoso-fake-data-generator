# src/dimensions/products/seeds/attribute_seeds.py

# -----------------------------------------------
# Category-based colors
# -----------------------------------------------
CATEGORY_COLORS = {
    "Audio": ["Black", "Silver", "Gray", "Blue"],
    "TV & Video": ["Black", "Silver", "Gray"],
    "Computers": ["Black", "Silver", "White", "Gray"],
    "Cameras and camcorders": ["Black", "Silver"],
    "Cell phones": ["Black", "Blue", "Silver", "White", "Gold"],
    "Music, Movies and Audio Books": ["Multi-color", "Blue", "Red"],
    "Games and Toys": ["Red", "Blue", "Green", "Yellow", "Multi-color"],
    "Home Appliances": ["White", "Silver", "Gray", "Black"],
}

# -----------------------------------------------
# Category descriptions
# -----------------------------------------------
CATEGORY_DESCRIPTIONS = {
    "Audio": [
        "High-fidelity sound with enhanced bass performance.",
        "Perfect for immersive music and studio-quality audio."
    ],
    "TV & Video": [
        "Cinematic picture quality with deep contrast and HDR.",
        "Designed for home entertainment with vivid color accuracy."
    ],
    "Computers": [
        "High-performance computing for professionals and gamers.",
        "Lightweight productivity machine with strong battery life."
    ],
    "Cameras and camcorders": [
        "Capture stunning images with fast autofocus and high dynamic range.",
        "Ideal for low-light photography and fast action scenes."
    ],
    "Cell phones": [
        "Premium device with long battery life and fast charging.",
        "Smooth performance with advanced camera system."
    ],
    "Music, Movies and Audio Books": [
        "Premium content in high-fidelity mastering.",
        "Perfect for entertainment enthusiasts."
    ],
    "Games and Toys": [
        "Fun and engaging design suitable for all ages.",
        "Great for developing strategic thinking and creativity."
    ],
    "Home Appliances": [
        "Designed for energy efficiency and long-term reliability.",
        "Smart appliance with multiple automation modes."
    ],
}

# -----------------------------------------------
# Brand class (Premium / Standard / Economy)
# -----------------------------------------------
BRAND_CLASS = {
    "Sony": "Premium",
    "Apple": "Premium",
    "Bose": "Premium",
    "KitchenAid": "Premium",
    "Bosch": "Premium",
    "Dyson": "Premium",

    "Samsung": "Standard",
    "LG": "Standard",
    "Dell": "Standard",
    "HP": "Standard",
    "Philips": "Standard",

    "Haier": "Economy",
    "Whirlpool": "Economy",

    "_default": "Standard",
}

# -----------------------------------------------
# Category weight ranges (kg)
# -----------------------------------------------
CATEGORY_WEIGHT = {
    "Audio": (0.1, 2.5),
    "TV & Video": (4, 30),
    "Computers": (1, 5),
    "Cameras and camcorders": (0.2, 1.5),
    "Cell phones": (0.12, 0.25),
    "Music, Movies and Audio Books": (0.05, 0.3),
    "Games and Toys": (0.1, 2.0),
    "Home Appliances": (2, 50),
}

# -----------------------------------------------
# Price ranges per category
# -----------------------------------------------
PRICE_RANGES = {
    "Audio": (20, 600),
    "TV & Video": (180, 2500),
    "Computers": (250, 3500),
    "Cameras and camcorders": (120, 3000),
    "Cell phones": (120, 1800),
    "Music, Movies and Audio Books": (5, 70),
    "Games and Toys": (8, 400),
    "Home Appliances": (60, 2000),
}

# -----------------------------------------------
# Stock Type per category
# -----------------------------------------------
CATEGORY_STOCK_TYPE = {
    "Audio": ["High", "High", "Medium", "Low"],
    "TV & Video": ["Medium", "Medium", "Low", "High"],
    "Computers": ["High", "Medium", "Low"],
    "Cameras and camcorders": ["Medium", "Low", "High"],
    "Cell phones": ["High", "Medium", "Low"],
    "Music, Movies and Audio Books": ["High", "Medium"],
    "Games and Toys": ["High", "Medium", "Low"],
    "Home Appliances": ["Medium", "Low", "High"],
}

# -----------------------------------------------
# Name: adjectives and series labels
# -----------------------------------------------
ADJECTIVES = ["Pro", "Ultra", "Smart", "Compact", "Prime", "Max", "Elite"]
SERIES = ["Series", "Gen", "Model", "X", "Z"]
