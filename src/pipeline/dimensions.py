import time
from pathlib import Path
from datetime import datetime
import os
from src.dimensions.customers import generate_synthetic_customers
from src.dimensions.promotions import generate_promotions_catalog
from src.dimensions.stores import generate_store_table
from src.dimensions.dates import generate_date_table
from src.dimensions.currency import generate_currency_dimension
from src.dimensions.exchange_rates import generate_exchange_rate_table
from src.dimensions.geography_builder import build_dim_geography

from src.utils.versioning import should_regenerate, save_version
from src.utils.logging_utils import stage, info, skip, done

from src.utils.fx_yahoo import build_or_update_fx
from src.facts.sales.sales_writer import write_delta_partitioned
import pandas as pd


def expand_date_ranges(ranges):
    """
    Convert date_ranges into precise per-year windows:
    [
        {2023: (2023-04-01, 2023-12-31)},
        {2024: (2024-01-01, 2024-02-01)},
    ]
    """
    year_windows = {}

    for r in ranges:
        s = datetime.fromisoformat(r["start"])
        e = datetime.fromisoformat(r["end"])

        for y in range(s.year, e.year + 1):

            # full default year range
            y_start = datetime(y, 1, 1)
            y_end   = datetime(y, 12, 31)

            # clamp to actual date_window
            if y == s.year:
                y_start = s
            if y == e.year:
                y_end = e

            year_windows[y] = (y_start, y_end)

    return year_windows


def generate_dimensions(cfg, parquet_dims: Path):

    def out(name):
        return parquet_dims / f"{name}.parquet"

    def changed(name, section):
        return should_regenerate(name, section, out(name))

    # --------------------------------------------------
    # Geography (root)
    # --------------------------------------------------
    if changed("geography", cfg["geography"]):
        with stage("Generating Geography"):
            build_dim_geography(cfg)
            save_version("geography", cfg["geography"])
            info(f"Saved geography → {out('geography')}")
    else:
        skip("Geography up-to-date; skipping regeneration")

    # --------------------------------------------------
    # Customers (depends on geography)
    # --------------------------------------------------
    customer_need = changed("customers", cfg["customers"]) or changed("geography", cfg["geography"])

    if customer_need:
        info("Dependency triggered: Customers will regenerate.")
        with stage("Generating Customers"):
            df = generate_synthetic_customers(cfg)
            df.to_parquet(out("customers"), index=False)
            save_version("customers", cfg["customers"])
            info(f"Saved customers → {out('customers')}")
    else:
        skip("Customers up-to-date; skipping regeneration")

    # --------------------------------------------------
    # Promotions (independent)
    # --------------------------------------------------
    if changed("promotions", cfg["promotions"]):
        with stage("Generating Promotions"):

            promo_cfg = cfg["promotions"]

            # 1) Use explicit promo date_ranges if provided
            ranges = promo_cfg.get("date_ranges", []) or []

            if ranges:
                # expand date ranges → precise year windows (existing helper)
                year_windows = expand_date_ranges(ranges)
                years = sorted(year_windows.keys())

            else:
                # Use global defaults.dates
                global_dates = cfg["defaults"]["dates"]
                start = global_dates["start"]
                end = global_dates["end"]

                # Apply override if present
                override_dates = promo_cfg.get("override", {}).get("dates", {})
                start = override_dates.get("start", start)
                end = override_dates.get("end", end)

                # Convert to year-based windows
                start_year = int(start[:4])
                end_year = int(end[:4])

                year_windows = {}
                for y in range(start_year, end_year + 1):
                    y_start = f"{y}-01-01"
                    y_end = f"{y}-12-31"

                    # Clamp window to actual global override start/end
                    if y == start_year:
                        y_start = start
                    if y == end_year:
                        y_end = end

                    # convert YYYY-MM-DD → datetime
                    ws = datetime.fromisoformat(y_start)
                    we = datetime.fromisoformat(y_end)

                    year_windows[y] = (ws, we)

                years = list(year_windows.keys())


            df = generate_promotions_catalog(
                years=years,
                year_windows=year_windows,
                num_seasonal=promo_cfg["num_seasonal"],
                num_clearance=promo_cfg["num_clearance"],
                num_limited=promo_cfg["num_limited"],
                seed=promo_cfg.get("seed", cfg.get("defaults", {}).get("seed")),
            )

            df.to_parquet(out("promotions"), index=False)
            save_version("promotions", promo_cfg)
            info(f"Saved promotions → {out('promotions')}")

    else:
        skip("Promotions up-to-date; skipping regeneration")



    # --------------------------------------------------
    # Stores (depends on geography)
    # --------------------------------------------------
    store_need = changed("stores", cfg["stores"]) or changed("geography", cfg["geography"])

    if store_need:
        info("Dependency triggered: Stores will regenerate.")
        with stage("Generating Stores"):

            base_seed = cfg["defaults"]["seed"]
            seed = cfg["stores"].get("override", {}).get("seed", cfg["stores"].get("seed", base_seed))

            df = generate_store_table(
                geography_parquet_path=os.path.join(parquet_dims, "geography.parquet"),
                num_stores=cfg["stores"]["num_stores"],
                opening_start=cfg["stores"]["opening"]["start"],
                opening_end=cfg["stores"]["opening"]["end"],
                closing_end=cfg["stores"]["closing_end"],
                seed=seed,
            )
            df.to_parquet(out("stores"), index=False)
            save_version("stores", cfg["stores"])
            info(f"Saved stores → {out('stores')}")

    # --------------------------------------------------
    # Dates
    # --------------------------------------------------
    if changed("dates", cfg["dates"]):
        with stage("Generating Dates"):
            base_start = cfg["defaults"]["dates"]["start"]
            base_end   = cfg["defaults"]["dates"]["end"]

            # Apply overrides
            ovr = cfg["dates"].get("override", {}).get("dates", {})
            start = ovr.get("start", base_start)
            end   = ovr.get("end",   base_end)

            df = generate_date_table(start, end, cfg["dates"]["fiscal_month_offset"])
            df.to_parquet(out("dates"), index=False)
            save_version("dates", cfg["dates"])
            info(f"Saved dates → {out('dates')}")
    else:
        skip("Dates up-to-date; skipping regeneration")


    # --------------------------------------------------
    # Currency
    # --------------------------------------------------
    if changed("currency", cfg["exchange_rates"]):
        with stage("Generating Currency Dimension"):
            df = generate_currency_dimension(cfg["exchange_rates"]["currencies"])
            df.to_parquet(out("currency"), index=False)
            save_version("currency", cfg["exchange_rates"])
            info(f"Saved currency → {out('currency')}")
    else:
        skip("Currency dimension up-to-date; skipping regeneration")

    # -----------------------------
    # Exchange Rates (use explicit FX dates if present)
    # -----------------------------
    from pathlib import Path
    import pandas as pd

    # -----------------------------
    # FX Start/End Priority Logic
    # -----------------------------

    # Defaults
    defaults_start = pd.to_datetime(cfg["defaults"]["dates"]["start"]).date()
    defaults_end   = pd.to_datetime(cfg["defaults"]["dates"]["end"]).date()

    # Optional FX-specific start date
    fx_dates_cfg = cfg.get("exchange_rates", {}).get("dates", {}) or {}
    fx_start_cfg = fx_dates_cfg.get("start", None)

    if fx_start_cfg:
        fx_start_cfg = pd.to_datetime(fx_start_cfg).date()
        fx_start = min(defaults_start, fx_start_cfg)   # priority rule
    else:
        fx_start = defaults_start

    # End date ALWAYS from defaults
    fx_end = defaults_end

    # normalize to pandas timestamps / date objects
    fx_start = pd.to_datetime(fx_start).date()
    fx_end   = pd.to_datetime(fx_end).date()

    # master path (full history)
    master_fx_path = parquet_dims / "exchange_rates.parquet"

    # determine if regeneration is needed (Option A behavior)
    if master_fx_path.exists():
        fx_df = pd.read_parquet(master_fx_path)
        fx_df["Date"] = pd.to_datetime(fx_df["Date"])
        last_fx_date = fx_df["Date"].max().date()
    else:
        last_fx_date = None

    # regenerate when missing or end not covered
    fx_need = (last_fx_date is None) or (last_fx_date < fx_end)

    if fx_need:
        info("Dependency triggered: Exchange Rates will regenerate.")
        with stage("Generating Exchange Rates"):
            # build or extend full history up to fx_end
            build_or_update_fx(
                start_date=fx_start.isoformat(),   # pass ISO string or date
                end_date=fx_end.isoformat(),
                out_path=parquet_dims / "exchange_rates.parquet"
            )

            # After build_or_update_fx returns, ensure we forward-fill missing calendar days
            # (if you already implemented ffill in fx_yahoo.py this will be redundant but safe)
            fx_full = pd.read_parquet(parquet_dims / "exchange_rates.parquet")
            fx_full["Date"] = pd.to_datetime(fx_full["Date"])

            # create full calendar and ffill per ToCurrency
            full_range = pd.date_range(start=fx_full["Date"].min(), end=fx_full["Date"].max(), freq="D")
            filled = []
            for cur in sorted(fx_full["ToCurrency"].unique()):
                sub = fx_full[fx_full["ToCurrency"] == cur].set_index("Date").sort_index()
                sub = sub.reindex(full_range)
                sub["FromCurrency"] = cfg["exchange_rates"].get("base_currency", "USD")
                sub["ToCurrency"] = cur
                # forward fill ExchangeRate; if leading NaNs remain, backfill (use last known before start)
                sub["ExchangeRate"] = sub["ExchangeRate"].ffill().bfill()
                filled.append(sub.reset_index().rename(columns={"index": "Date"}))
            fx_filled = pd.concat(filled, ignore_index=True)
            fx_filled.sort_values(["Date", "ToCurrency"], inplace=True)

            # save final master (overwrite)
            fx_filled.to_parquet(parquet_dims / "exchange_rates.parquet", index=False)

            # copy to final exchange_rates.parquet (no slicing)
            final_path = parquet_dims / "exchange_rates.parquet"
            Path(final_path).write_bytes(Path(parquet_dims / "exchange_rates.parquet").read_bytes())

            save_version("exchange_rates", cfg["exchange_rates"])
            info(f"Saved exchange_rates → {final_path}")

    else:
        skip("Exchange Rates up-to-date; skipping regeneration")
