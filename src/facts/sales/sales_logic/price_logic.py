import numpy as np
from src.facts.sales.sales_logic.globals import State


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _round_price_endings(price, rng):
    endings = np.array([0.99, 0.95])
    base = np.floor(price)
    return base + rng.choice(endings, size=price.shape)

def _round_bucket(values, size):
    return np.round(values / size) * size

def _floor_bucket(values, size):
    return np.maximum(size, np.floor(values / size) * size)

def _micro_adjust(values, scale):
    eps = (np.mod(values * 100, 7) - 3) * scale
    return np.round(values + eps, 2)

def _quantize(values, decimals=4):
    return np.round(values.astype(np.float64), decimals)

def compute_prices(rng, n, unit_price, unit_cost, promo_pct):
    """
    Optimized price computation:
    - Cache State locally
    - Reduce repeated attribute lookups
    - Delay rounding where safe
    """

    # -------------------------------------------------
    # CACHE STATE LOCALLY (HOT PATH OPTIMIZATION)
    # -------------------------------------------------
    S = State

    pricing_mode = S.pricing_mode or "random"
    discrete = bool(S.discrete_factors)

    bucket_size = S.bucket_size or 0.25
    unit_bucket = S.unit_bucket_size or 1.00
    disc_bucket = S.discount_bucket_size or 0.50

    enforce_margin = bool(S.enforce_min_price)
    min_margin_pct = 0.05

    decimals_mode = S.decimals_mode or "off"
    decimals_scale = S.decimals_scale or 0.02

    retail_endings = bool(S.retail_price_endings)

    min_price = getattr(S, "min_unit_price", None)
    max_price = getattr(S, "max_unit_price", None)

    value_scale = getattr(S, "value_scale", 1.0)

    # -------------------------------------------------
    # 1. BASE LIST PRICE
    # -------------------------------------------------
    base_unit_price = unit_price * value_scale

    # -------------------------------------------------
    # 2. SALES DISCOUNT
    # -------------------------------------------------
    sales_disc_pct = rng.choice(
        [0, 2, 5, 10],
        size=n,
        p=[0.45, 0.25, 0.20, 0.10]
    )

    base_discount = base_unit_price * (sales_disc_pct / 100.0)
    base_discount = np.minimum(base_discount, base_unit_price * 0.30)

    # -------------------------------------------------
    # 2a. SMALL INCIDENTAL DISCOUNTS
    # -------------------------------------------------
    zero_mask = base_discount == 0
    if zero_mask.any():
        apply_small = rng.random(zero_mask.sum()) < 0.65
        base_discount[zero_mask] += apply_small * rng.choice(
            [0.5, 1.0],
            size=zero_mask.sum(),
            p=[0.7, 0.3]
        )

    # -------------------------------------------------
    # 3. COST (CONTROLLED MARGIN)
    # -------------------------------------------------
    margin_pct = rng.uniform(0.02, 0.08, size=n)
    base_unit_cost = base_unit_price * (1 - margin_pct)

    # -------------------------------------------------
    # 4. NET PRICE
    # -------------------------------------------------
    base_net_price = base_unit_price - base_discount

    # -------------------------------------------------
    # 5. MARGIN PROTECTION
    # -------------------------------------------------
    if enforce_margin:
        min_net = base_unit_cost * (1 + min_margin_pct)
        base_net_price = np.maximum(base_net_price, min_net)

    # -------------------------------------------------
    # 6. DISCRETE / BUCKETED STRUCTURE
    # -------------------------------------------------
    if pricing_mode == "discrete" or discrete:
        base_unit_price = _round_bucket(base_unit_price, unit_bucket)
        base_discount   = _round_bucket(base_discount, disc_bucket)
        base_unit_cost  = _floor_bucket(base_unit_cost, unit_bucket)
        base_net_price  = base_unit_price - base_discount

    elif pricing_mode == "bucketed":
        base_unit_price = _round_bucket(base_unit_price, bucket_size)
        base_discount   = _round_bucket(base_discount, bucket_size)
        base_unit_cost  = _floor_bucket(base_unit_cost, bucket_size)
        base_net_price  = base_unit_price - base_discount

    # -------------------------------------------------
    # 7. RETAIL ENDINGS
    # -------------------------------------------------
    if retail_endings or decimals_mode == "strict":
        base_unit_price = _round_price_endings(base_unit_price, rng)
        base_net_price  = base_unit_price - base_discount

    # -------------------------------------------------
    # 8. MICRO DECIMALS
    # -------------------------------------------------
    if decimals_mode == "micro":
        final_unit_price = _micro_adjust(base_unit_price, decimals_scale)
        discount_amt     = _micro_adjust(base_discount, decimals_scale)
        final_unit_cost  = _micro_adjust(base_unit_cost, decimals_scale)
        final_net_price  = _micro_adjust(base_net_price, decimals_scale)
    else:
        final_unit_price = base_unit_price
        discount_amt     = base_discount
        final_unit_cost  = base_unit_cost
        final_net_price  = base_net_price

    # -------------------------------------------------
    # 9. HARD LIMITS
    # -------------------------------------------------
    if min_price is not None:
        final_unit_price = np.maximum(final_unit_price, min_price)

    if max_price is not None:
        final_unit_price = np.minimum(final_unit_price, max_price)

    # -------------------------------------------------
    # 10. RE-DERIVE DISCOUNT & NET
    # -------------------------------------------------
    if min_price is not None:
        max_discount = final_unit_price - min_price
    else:
        max_discount = final_unit_price

    discount_amt = np.clip(discount_amt, 0, max_discount)
    final_net_price = final_unit_price - discount_amt

    if min_price is not None:
        final_net_price = np.maximum(final_net_price, min_price)

    if max_price is not None:
        final_net_price = np.minimum(final_net_price, max_price)

    # -------------------------------------------------
    # 11. COST SAFETY
    # -------------------------------------------------
    final_unit_cost = np.minimum(final_unit_cost, final_net_price)
    final_unit_cost = np.clip(final_unit_cost, 0.01, None)

    # -------------------------------------------------
    # 12. FINAL QUANTIZATION (ONCE)
    # -------------------------------------------------
    final_unit_price = _quantize(final_unit_price, 4)
    discount_amt     = _quantize(discount_amt, 4)
    final_unit_cost  = _quantize(final_unit_cost, 4)
    final_net_price  = _quantize(final_net_price, 4)

    return {
        "discount_amt": discount_amt,
        "final_unit_price": final_unit_price,
        "final_unit_cost": final_unit_cost,
        "final_net_price": final_net_price,
    }
