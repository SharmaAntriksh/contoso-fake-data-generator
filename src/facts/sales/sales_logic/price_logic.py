import numpy as np
from src.facts.sales.sales_logic.globals import State

# Allow discounts to eat into margin up to this factor
MAX_DISCOUNT_COST_MULTIPLIER = 0.90

# -------------------------------------------------
# Discount ladder (intentional, weighted)
# Values are in USD (pre value_scale)
# -------------------------------------------------
DISCOUNT_LADDER = [
    ("none", 0.00, 60),

    # percentage discounts (most common)
    ("pct",  0.05, 12),
    ("pct",  0.10, 10),
    ("pct",  0.15, 8),
    ("pct",  0.20, 6),
    ("pct",  0.30, 4),

    # absolute USD discounts (rare, high impact)
    ("abs",  5,    6),
    ("abs",  10,   5),
    ("abs",  25,   3),
    ("abs",  50,   2),
    ("abs",  75,   1),
    ("abs",  100,  1),
]

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _round_bucket(values, size):
    return np.round(values / size) * size

def _micro_adjust(values, scale=0.02):
    eps = (np.mod(values * 100, 7) - 3) * scale
    return np.maximum(0, np.round(values + eps, 2))

def _quantize(values, decimals=4):
    return np.round(values.astype(np.float64), decimals)


def compute_prices(rng, n, unit_price, unit_cost, promo_pct):
    S = State

    # -------------------------------
    # CONFIG
    # -------------------------------
    mode = S.pricing_mode or "bucketed"
    bucket = S.bucket_size or 0.25

    min_price = S.min_unit_price
    max_price = S.max_unit_price

    decimals = S.decimals_mode or "off"
    value_scale = S.value_scale if S.value_scale is not None else 1.0
    if value_scale <= 0:
        raise ValueError(f"value_scale must be > 0, got {value_scale}")

    # -------------------------------
    # 1. BASE PRICE
    # -------------------------------
    base_price = unit_price.copy()

    # -------------------------------
    # 2. DISCOUNT LOGIC (LADDER-BASED)
    # -------------------------------
    types, values, weights = zip(*DISCOUNT_LADDER)
    weights = np.array(weights, dtype=np.float64)
    weights /= weights.sum()

    choices = rng.choice(len(DISCOUNT_LADDER), size=n, p=weights)

    discount_amt = np.zeros(n, dtype=np.float64)

    for i, idx in enumerate(choices):
        dtype = types[idx]
        dval = values[idx]

        if dtype == "pct":
            discount_amt[i] = base_price[i] * dval
        elif dtype == "abs":
            discount_amt[i] = dval
        # "none" → 0 discount

    # -------------------------------
    # 3. NET PRICE
    # -------------------------------
    net_price = base_price - discount_amt

    # -------------------------------
    # 4. COST & BASE MARGIN (HEALTHY)
    # -------------------------------
    # Higher baseline margins so profits are visible in aggregates
    margin_pct = rng.uniform(0.15, 0.35, size=n)   # 15%–35% margin
    cost = base_price * (1 - margin_pct)

    # Allow discounts, but keep cost anchored
    net_price = np.maximum(net_price, cost * MAX_DISCOUNT_COST_MULTIPLIER)

    # -------------------------------
    # 5. STRUCTURE (BUCKETS)
    # -------------------------------
    if mode == "bucketed":
        base_price = _round_bucket(base_price, bucket)
        discount_amt = _round_bucket(discount_amt, bucket)
        cost = _round_bucket(cost, bucket)
        net_price = base_price - discount_amt

# -------------------------------
    # 6. FINAL MARGIN SAFETY (LOSS-LEADER AWARE)
    # -------------------------------
    net_price = np.maximum(net_price, cost * MAX_DISCOUNT_COST_MULTIPLIER)
    net_price = np.minimum(net_price, base_price)

    discount_amt = base_price - net_price
    discount_amt = np.clip(discount_amt, 0, base_price)

    # -------------------------------
    # 7. FINAL VALUE SCALING
    # -------------------------------
    base_price *= value_scale
    net_price *= value_scale
    cost *= value_scale
    # discount_amt *= value_scale

    # -------------------------------
    # 8. DECIMALS (COSMETIC ONLY)
    # -------------------------------
    if decimals == "micro":
        base_price = _micro_adjust(base_price)
        net_price = _micro_adjust(net_price)
        cost = _micro_adjust(cost)
        discount_amt = _micro_adjust(discount_amt)

    # -------------------------------
    # 9. FINAL HARD CONSTRAINTS (VISIBLE PRICES)
    # -------------------------------
    if min_price is not None:
        base_price = np.maximum(base_price, min_price)
        net_price = np.maximum(net_price, min_price)

    if max_price is not None:
        base_price = np.minimum(base_price, max_price)
        net_price = np.minimum(net_price, max_price)

    net_price = np.minimum(net_price, base_price)
    discount_amt = base_price - net_price

    cost = np.clip(cost, 0, None)
    discount_amt = np.clip(discount_amt, 0, base_price)

    # -------------------------------
    # 10. FINAL COST VISIBILITY SAFETY
    # -------------------------------
    cost = np.minimum(cost, net_price)

    # -------------------------------
    # 11. FINAL ROUNDING
    # -------------------------------
    return {
        "final_unit_price": _quantize(base_price),
        "final_net_price": _quantize(net_price),
        "final_unit_cost": _quantize(cost),
        "discount_amt": _quantize(discount_amt),
    }
