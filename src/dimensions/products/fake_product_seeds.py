# -------------------------------------------------------------
# Static seeds for fake product generation
# -------------------------------------------------------------

# Category → Subcategories
CATEGORIES = {
    "Audio": [
        "Headphones",
        "Speakers",
        "Soundbars",
        "Home Audio Systems",
        "Car Audio"
    ],
    "TV & Video": [
        "LED TVs",
        "OLED TVs",
        "Projectors",
        "Streaming Devices",
        "Blu-ray/DVD Players"
    ],
    "Computers": [
        "Laptops",
        "Desktops",
        "Monitors",
        "Computer Accessories",
        "Networking Equipment"
    ],
    "Cameras & Camcorders": [
        "Digital Cameras",
        "DSLR Cameras",
        "Mirrorless Cameras",
        "Camcorders",
        "Camera Accessories"
    ],
    "Cell Phones": [
        "Smartphones",
        "Feature Phones",
        "Phone Accessories",
        "Power Banks",
        "Wearable Connectivity"
    ],
    "Music, Movies & Audio Books": [
        "Music CDs",
        "Movie DVDs",
        "Blu-ray Movies",
        "Audio Books",
        "Digital Media Cards"
    ],
    "Games & Toys": [
        "Console Games",
        "Board Games",
        "PC Games",
        "Action Figures",
        "Toys & Learning Kits"
    ],
    "Home Appliances": [
        "Refrigerators",
        "Washing Machines",
        "Air Conditioners",
        "Microwave Ovens",
        "Vacuum Cleaners"
    ],

    # Additional enriched categories
    "Wearables": [
        "Smartwatches",
        "Fitness Bands",
        "VR Headsets",
        "Smart Glasses",
        "Health Trackers"
    ],
    "Smart Home": [
        "Smart Lights",
        "Smart Plugs",
        "Smart Cameras",
        "Smart Thermostats",
        "Voice Assistants"
    ],
    "Office Supplies": [
        "Printers",
        "Scanners",
        "Office Furniture",
        "Writing Supplies",
        "Storage & Organization"
    ],
    "Furniture": [
        "Sofas",
        "Beds",
        "Chairs",
        "Tables",
        "Storage Units"
    ],
    "Kitchen & Dining": [
        "Cookware",
        "Tableware",
        "Small Kitchen Appliances",
        "Storage Containers",
        "Cutlery"
    ],
    "Personal Care": [
        "Grooming Devices",
        "Hair Care Tools",
        "Oral Care",
        "Massage Devices",
        "Beauty Tools"
    ],
    "Sports & Outdoors": [
        "Fitness Equipment",
        "Outdoor Gear",
        "Sports Apparel",
        "Cycling",
        "Camping Equipment"
    ],
}

# -------------------------------------------------------------
# Base product names per subcategory
# -------------------------------------------------------------

BASE_PRODUCT_NAMES = {
    # Audio
    "Headphones": ["Headphones", "Wireless Headphones", "Noise-Canceling Headphones"],
    "Speakers": ["Bluetooth Speaker", "Portable Speaker", "Home Speaker"],
    "Soundbars": ["Soundbar"],
    "Home Audio Systems": ["Audio System", "Stereo System"],
    "Car Audio": ["Car Speaker", "Car Woofer", "Car Audio Deck"],

    # TV & Video
    "LED TVs": ["LED TV"],
    "OLED TVs": ["OLED TV"],
    "Projectors": ["Projector", "Mini Projector"],
    "Streaming Devices": ["Streaming Stick", "Media Box"],
    "Blu-ray/DVD Players": ["Blu-ray Player", "DVD Player"],

    # Computers
    "Laptops": ["Laptop", "Notebook", "Ultrabook"],
    "Desktops": ["Desktop", "Tower PC", "Mini PC"],
    "Monitors": ["Monitor", "Gaming Monitor"],
    "Computer Accessories": ["Keyboard", "Mouse", "Webcam", "Docking Station"],
    "Networking Equipment": ["Router", "WiFi Extender", "Network Switch"],

    # Cameras & Camcorders
    "Digital Cameras": ["Digital Camera"],
    "DSLR Cameras": ["DSLR Camera"],
    "Mirrorless Cameras": ["Mirrorless Camera"],
    "Camcorders": ["Camcorder"],
    "Camera Accessories": ["Camera Lens", "Tripod", "Camera Flash"],

    # Cell Phones
    "Smartphones": ["Smartphone"],
    "Feature Phones": ["Feature Phone"],
    "Phone Accessories": ["Phone Case", "Charger", "Wireless Charger", "Earbuds"],
    "Power Banks": ["Power Bank"],
    "Wearable Connectivity": ["Smartwatch", "Smart Band"],

    # Music, Movies & Audio Books
    "Music CDs": ["Music CD"],
    "Movie DVDs": ["DVD Movie"],
    "Blu-ray Movies": ["Blu-ray Movie"],
    "Audio Books": ["Audio Book"],
    "Digital Media Cards": ["Media Card"],

    # Games & Toys
    "Console Games": ["Console Game"],
    "Board Games": ["Board Game"],
    "PC Games": ["PC Game"],
    "Action Figures": ["Action Figure"],
    "Toys & Learning Kits": ["Learning Kit", "Toy Set"],

    # Home Appliances
    "Refrigerators": ["Refrigerator"],
    "Washing Machines": ["Washing Machine"],
    "Air Conditioners": ["Air Conditioner"],
    "Microwave Ovens": ["Microwave Oven"],
    "Vacuum Cleaners": ["Vacuum Cleaner", "Robot Vacuum"],

    # Wearables
    "Smartwatches": ["Smartwatch"],
    "Fitness Bands": ["Fitness Band"],
    "VR Headsets": ["VR Headset"],
    "Smart Glasses": ["Smart Glasses"],
    "Health Trackers": ["Health Tracker"],

    # Smart Home
    "Smart Lights": ["Smart Light"],
    "Smart Plugs": ["Smart Plug"],
    "Smart Cameras": ["Smart Camera"],
    "Smart Thermostats": ["Smart Thermostat"],
    "Voice Assistants": ["Voice Assistant"],

    # Office Supplies
    "Printers": ["Printer"],
    "Scanners": ["Scanner"],
    "Office Furniture": ["Office Chair", "Office Desk"],
    "Writing Supplies": ["Pen Set", "Marker Set"],
    "Storage & Organization": ["Filing Box", "Organizer"],

    # Furniture
    "Sofas": ["Sofa", "Reclining Sofa"],
    "Beds": ["Bed Frame", "Mattress"],
    "Chairs": ["Chair", "Ergonomic Chair"],
    "Tables": ["Table", "Coffee Table", "Dining Table"],
    "Storage Units": ["Cabinet", "Bookshelf"],

    # Kitchen & Dining
    "Cookware": ["Cookware Set", "Frying Pan", "Saucepan"],
    "Tableware": ["Dinner Set", "Plate Set"],
    "Small Kitchen Appliances": ["Blender", "Toaster", "Mixer"],
    "Storage Containers": ["Food Container"],
    "Cutlery": ["Cutlery Set"],

    # Personal Care
    "Grooming Devices": ["Trimmer", "Shaver"],
    "Hair Care Tools": ["Hair Dryer", "Hair Straightener"],
    "Oral Care": ["Electric Toothbrush"],
    "Massage Devices": ["Massage Gun"],
    "Beauty Tools": ["Face Steamer", "Epilator"],

    # Sports & Outdoors
    "Fitness Equipment": ["Dumbbell Set", "Treadmill", "Exercise Bike"],
    "Outdoor Gear": ["Tent", "Backpack"],
    "Sports Apparel": ["Sports T-shirt", "Track Pants"],
    "Cycling": ["Bicycle", "Helmet"],
    "Camping Equipment": ["Sleeping Bag", "Camping Stove"],
}

# -------------------------------------------------------------
# Name construction patterns
# -------------------------------------------------------------

ADJECTIVES = [
    "Pro", "Ultra", "Smart", "Compact", "Prime", "Max", "Elite"
]

SERIES = [
    "Series", "Gen", "Model", "X", "Z"
]
