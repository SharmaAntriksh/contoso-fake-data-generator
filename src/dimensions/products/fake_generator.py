# src/dimensions/products/fake_generator.py
import math
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime as dt

# Seeds (adjust module paths if different)
from .seeds.base_names import BASE_PRODUCT_NAMES
from .seeds.attribute_seeds import ADJECTIVES, SERIES, PRICE_RANGES
from .seeds.category_brands import CATEGORY_BRANDS
from .seeds.fallback_brands import BRANDS
from .seeds.brand_model_patterns import BRAND_MODEL_PATTERNS
from .seeds.attribute_seeds import CATEGORY_COLORS, CATEGORY_STOCK_TYPE, CATEGORY_WEIGHT
from .seeds.attribute_seeds import BRAND_CLASS

# -----------------------------
# Helpers
# -----------------------------
def _choice_rng(arr, size, rng):
    arr = np.asarray(arr, dtype=object)
    idx = rng.integers(0, len(arr), size=size)
    return arr[idx]

def _gen_ean13_batch(n, rng):
    # generate 12 random digits then compute checksum per EAN-13 spec
    digits = rng.integers(0, 10, size=(n, 12)).astype(int)
    # positions: index 0..11 -> weights: odd idx (from left, 1-based) weight=1, even weight=3
    # compute checksum
    weights = np.array([1 if (i % 2 == 0) else 3 for i in range(12)], dtype=int)
    sums = (digits * weights).sum(axis=1)
    checksum = (10 - (sums % 10)) % 10
    full = np.concatenate([digits, checksum.reshape(-1, 1)], axis=1)
    # join to strings
    return np.array([''.join(map(str, row)) for row in full], dtype=object)

def _gen_release_discontinued(n, rng, start_year=2015, end_year=2023):
    years = rng.integers(start_year, end_year + 1, size=n)
    months = rng.integers(1, 13, size=n)
    days = rng.integers(1, 28, size=n)
    releases = [dt.date(int(y), int(m), int(d)).isoformat() for y,m,d in zip(years, months, days)]
    # discontinued with 20% probability -> add random days 180..2000
    prob = rng.random(size=n)
    discontinued = []
    for i, p in enumerate(prob):
        if p < 0.2:
            add_days = int(rng.integers(180, 2001))
            y = years[i]; mo = months[i]; da = days[i]
            rc = dt.date(int(y), int(mo), int(da)) + dt.timedelta(days=add_days)
            discontinued.append(rc.isoformat())
        else:
            discontinued.append(None)
    return np.array(releases, dtype=object), np.array(discontinued, dtype=object)

def _model_numbers_for_brands(brands_arr, rng):
    # brands_arr: numpy array of brand strings
    # use BRAND_MODEL_PATTERNS map (callable) if present, fallback to MDL-####
    n = len(brands_arr)
    model_arr = np.empty(n, dtype=object)
    unique_brands, inv = np.unique(brands_arr, return_inverse=True)
    for i, ub in enumerate(unique_brands):
        pattern_fn = BRAND_MODEL_PATTERNS.get(ub, BRAND_MODEL_PATTERNS.get("_default"))
        count = np.sum(inv == i)
        # call pattern_fn count times
        model_arr[inv == i] = [pattern_fn() for _ in range(count)]
    return model_arr

# -----------------------------
# Main generator (streaming, vectorized)
# -----------------------------
def generate_fake_products(config: dict, output_folder: Path, batch_size: int = 100_000):
    """
    Generate rich fake product dimension in vectorized streaming batches.
    Writes output_folder / 'products.parquet' and returns Path.
    """
    num_products = int(config.get("num_products", 100_000))
    seed = int(config.get("seed", 42))
    rng = np.random.default_rng(seed)

    out_path = Path(output_folder) / "products.parquet"

    # Load lookups once
    df_sub = pd.read_parquet(output_folder / "product_subcategory.parquet")
    df_cat = pd.read_parquet(output_folder / "product_category.parquet")

    sub_keys = df_sub["SubcategoryKey"].to_numpy()
    sub_names = df_sub["Subcategory"].to_numpy()
    sub_cat_keys = df_sub["CategoryKey"].to_numpy()

    # category lookup: key->name
    cat_series = df_cat.set_index("CategoryKey")["Category"]

    # Precompute category -> brands array
    category_to_brands = {}
    for cat in df_cat["Category"].unique():
        category_to_brands[cat] = np.array(CATEGORY_BRANDS.get(cat, BRANDS), dtype=object)

    # Parquet writer
    batches = math.ceil(num_products / batch_size) if (b := num_products // batch_size) is None else math.ceil(num_products / batch_size)
    writer = None
    next_pk = 1

    for batch_idx in range(math.ceil(num_products / batch_size)):
        this_batch = batch_size if (batch_idx < math.ceil(num_products / batch_size) - 1) else (num_products - batch_idx * batch_size)
        # pick random subcategory indices for the batch
        sub_idx = rng.integers(0, len(sub_keys), size=this_batch)

        sub_key_arr = sub_keys[sub_idx].astype(int)
        sub_name_arr = sub_names[sub_idx].astype(object)
        cat_key_arr = sub_cat_keys[sub_idx].astype(int)

        # map category names
        cat_name_arr = pd.Series(cat_key_arr).map(cat_series).to_numpy()

        # brand selection vectorized per category
        brand_arr = np.empty(this_batch, dtype=object)
        unique_cats, inv_cat = np.unique(cat_name_arr, return_inverse=True)
        for i, uc in enumerate(unique_cats):
            pos = np.nonzero(inv_cat == i)[0]
            brands_for_cat = category_to_brands.get(uc, np.array(BRANDS, dtype=object))
            brand_arr[pos] = _choice_rng(brands_for_cat, size=len(pos), rng=rng)

        # base product names per subcategory
        base_arr = np.empty(this_batch, dtype=object)
        unique_subs, inv_sub = np.unique(sub_name_arr, return_inverse=True)
        for i, us in enumerate(unique_subs):
            pos = np.nonzero(inv_sub == i)[0]
            base_list = np.array(BASE_PRODUCT_NAMES.get(us, [us]), dtype=object)
            base_arr[pos] = _choice_rng(base_list, size=len(pos), rng=rng)

        # adjectives, series, model nums
        adj_arr = _choice_rng(np.array(ADJECTIVES, dtype=object), size=this_batch, rng=rng)
        series_arr = _choice_rng(np.array(SERIES, dtype=object), size=this_batch, rng=rng)
        model_num_arr = rng.integers(1000, 9999, size=this_batch).astype(str)

        # product name
        # Format: "{Brand} {Base} {Adj} {Series} {ModelNum}"
        product_name_arr = np.char.add(np.char.add(np.char.add(np.char.add(brand_arr, " "), base_arr), " "), np.char.add(np.char.add(adj_arr, " "), np.char.add(series_arr, " ")))
        product_name_arr = np.char.add(product_name_arr, model_num_arr)

        # product code (zero padded)
        product_keys = np.arange(next_pk, next_pk + this_batch, dtype=np.int64)
        product_code_arr = np.char.zfill(product_keys.astype(str), 7)

        # descriptions
        product_desc_arr = np.char.add("High quality ", np.char.add(base_arr, " for everyday use."))

        # model numbers using brand patterns
        model_numbers = _model_numbers_for_brands(brand_arr, rng)

        # EAN13
        ean13_arr = _gen_ean13_batch(this_batch, rng)

        # release/discontinued
        release_arr, discontinued_arr = _gen_release_discontinued(this_batch, rng)

        # Color per category
        color_arr = np.empty(this_batch, dtype=object)
        for i, cat in enumerate(cat_name_arr):
            color_choices = CATEGORY_COLORS.get(cat, ["Black"])
            color_arr[i] = rng.choice(np.array(color_choices, dtype=object))

        # weight per category (float)
        weight_arr = np.empty(this_batch, dtype=float)
        for i, cat in enumerate(cat_name_arr):
            low, high = CATEGORY_WEIGHT.get(cat, (0.1, 2.0))
            weight_arr[i] = round(float(rng.uniform(low, high)), 3)

        # stock types
        stock_arr = np.empty(this_batch, dtype=object)
        stock_code_arr = np.empty(this_batch, dtype=object)
        for i, cat in enumerate(cat_name_arr):
            options = CATEGORY_STOCK_TYPE.get(cat, ["High"])
            sel = rng.choice(np.array(options, dtype=object))
            stock_arr[i] = sel
            stock_code_arr[i] = str(options.index(sel) + 1)

        # class from brand
        class_arr = np.array([BRAND_CLASS.get(b, BRAND_CLASS.get("_default", "Standard")) for b in brand_arr], dtype=object)

        # pricing
        low_arr = np.empty(this_batch, dtype=float)
        high_arr = np.empty(this_batch, dtype=float)
        for i, cat in enumerate(cat_name_arr):
            l, h = PRICE_RANGES.get(cat, (20.0, 200.0))
            low_arr[i] = l
            high_arr[i] = h
        price_arr = np.round(rng.uniform(low_arr, high_arr), 2)
        cost_arr = np.round(price_arr * rng.uniform(0.5, 0.85, size=this_batch), 2)

        # assemble batch df
        batch_df = pd.DataFrame({
            "ProductKey": product_keys,
            "ProductCode": product_code_arr,
            "ProductName": product_name_arr,
            "ProductDescription": product_desc_arr,
            "SubcategoryKey": sub_key_arr,
            # "CategoryKey": cat_key_arr,
            "Brand": brand_arr,
            "Class": class_arr,
            "ModelNumber": model_numbers,
            "EAN13": ean13_arr,
            "ReleaseDate": release_arr,
            "DiscontinuedDate": discontinued_arr,
            "Color": color_arr,
            "WeightKg": weight_arr,
            "StockTypeCode": stock_code_arr,
            "StockType": stock_arr,
            "UnitCost": cost_arr,
            "UnitPrice": price_arr,
        })

        next_pk += this_batch

        # write batch
        table = pa.Table.from_pandas(batch_df)
        if writer is None:
            writer = pq.ParquetWriter(str(out_path), table.schema, use_dictionary=True, compression="snappy")
        writer.write_table(table)

    if writer is not None:
        writer.close()

    return out_path
