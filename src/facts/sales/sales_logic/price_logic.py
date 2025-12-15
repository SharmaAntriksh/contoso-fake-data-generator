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


# -------------------------------------------------
# Main
# -------------------------------------------------
def compute_prices(rng, n, unit_price, unit_cost, promo_pct):
    print(
        "DECIMAL MODE CHECK →",
        State.decimals_mode,
        type(State.decimals_mode)
    )

    # -------------------------------------------------
    # CONFIG (FLAT STATE)
    # -------------------------------------------------
    pricing_mode = State.pricing_mode or "random"
    discrete = bool(State.discrete_factors)

    bucket_size = State.bucket_size or 0.25
    unit_bucket = State.unit_bucket_size or 1.00
    disc_bucket = State.discount_bucket_size or 0.50

    enforce_margin = bool(State.enforce_min_price)
    min_margin_pct = 0.05

    decimals_mode = State.decimals_mode or "off"
    decimals_scale = State.decimals_scale or 0.02

    retail_endings = bool(State.retail_price_endings)

    # -------------------------------------------------
    # 1. LIST PRICE
    # -------------------------------------------------
    base_unit_price = np.round(unit_price, 2)

    # -------------------------------------------------
    # 2. SALES DISCOUNT
    # -------------------------------------------------
    sales_disc_pct = rng.choice(
        [0, 2, 5, 10],
        size=n,
        p=[0.70, 0.15, 0.10, 0.05]
    )

    base_discount = np.round(base_unit_price * sales_disc_pct / 100.0, 2)
    base_discount = np.minimum(base_discount, base_unit_price * 0.30)

    # -------------------------------------------------
    # 3. COST
    # -------------------------------------------------
    base_unit_cost = np.round(unit_cost, 2)

    # -------------------------------------------------
    # 4. NET PRICE (STRUCTURAL)
    # -------------------------------------------------
    base_net_price = np.round(base_unit_price - base_discount, 2)

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
        base_net_price  = np.round(base_unit_price - base_discount, 2)

    elif pricing_mode == "bucketed":
        base_unit_price = _round_bucket(base_unit_price, bucket_size)
        base_discount   = _round_bucket(base_discount, bucket_size)
        base_unit_cost  = _floor_bucket(base_unit_cost, bucket_size)
        base_net_price  = np.round(base_unit_price - base_discount, 2)

    # -------------------------------------------------
    # 7. RETAIL ENDINGS (STRUCTURAL)
    # -------------------------------------------------
    if retail_endings or decimals_mode == "strict":
        base_unit_price = _round_price_endings(base_unit_price, rng)
        base_net_price  = np.round(base_unit_price - base_discount, 2)

    # -------------------------------------------------
    # 8. MICRO DECIMALS (FINAL PRESENTATION ONLY)
    # -------------------------------------------------
    if decimals_mode == "micro":
        final_unit_price = _micro_adjust(base_unit_price, decimals_scale)
        discount_amt     = _micro_adjust(base_discount, decimals_scale)
        discount_amt     = np.clip(discount_amt, 0, None)
        final_unit_cost  = _micro_adjust(base_unit_cost, decimals_scale)
        final_net_price  = _micro_adjust(base_net_price, decimals_scale)
    else:
        final_unit_price = base_unit_price
        discount_amt     = base_discount
        final_unit_cost  = base_unit_cost
        final_net_price  = base_net_price

    # -------------------------------------------------
    # 9. SAFETY
    # -------------------------------------------------
    final_unit_cost = np.clip(final_unit_cost, 0.01, None)
    final_net_price = np.clip(final_net_price, 0.01, None)

    return {
        "discount_amt": discount_amt,
        "final_unit_price": final_unit_price,
        "final_unit_cost": final_unit_cost,
        "final_net_price": final_net_price,
    }
