import numpy as np

def apply_promotions(
    rng, n, order_dates,
    promo_keys_all, promo_pct_all,
    promo_start_all, promo_end_all,
    no_discount_key=1
):
    promo_keys = np.full(n, no_discount_key, dtype=np.int64)
    promo_pct = np.zeros(n, dtype=np.float64)

    if promo_keys_all is None or promo_keys_all.size == 0:
        return promo_keys, promo_pct

    # ------------------------------------------------------------
    # Build active promotion mask (vectorized)
    # ------------------------------------------------------------
    od = order_dates[:, None]
    active_mask = (od >= promo_start_all) & (od <= promo_end_all)

    # Ignore no-discount promos up front
    valid_mask = active_mask & (promo_keys_all != no_discount_key)

    # Rows with at least one valid promotion
    rows = np.nonzero(valid_mask.any(axis=1))[0]

    # ------------------------------------------------------------
    # Select one promo per valid row
    # ------------------------------------------------------------
    for i in rows:
        choices = np.flatnonzero(valid_mask[i])
        j = rng.choice(choices)
        promo_keys[i] = promo_keys_all[j]
        promo_pct[i] = promo_pct_all[j]

    return promo_keys, promo_pct
