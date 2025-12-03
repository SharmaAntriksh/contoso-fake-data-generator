import copy
import json
import yaml
from datetime import datetime
from pathlib import Path


def load_config_file(path):
    """
    Load config from a JSON or YAML file.
    Auto-detects format based on extension.
    """
    path = Path(path)
    ext = path.suffix.lower()

    if ext in (".yaml", ".yml"):
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    if ext == ".json":
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # auto-detect fallback
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read().strip()
        try:
            return yaml.safe_load(txt)
        except Exception:
            return json.loads(txt)


def load_config(cfg):
    """
    Returns a dict of fully-resolved module configs:
        resolved["sales"]
        resolved["promotions"]
        resolved["customers"]
        etc.
    """
    resolved = {}
    defaults = cfg.get("defaults", {})

    for section_name, section in cfg.items():
        if section_name == "defaults":
            continue

        # Skip non-dict values (like final_output_folder)
        if not isinstance(section, dict):
            resolved[section_name] = section
            continue

        resolved[section_name] = resolve_section(cfg, section_name, defaults)

    return resolved


def resolve_section(cfg, section_name, defaults):
    """
    Merge: defaults → section → override
    + special logic for promotions.date_ranges
    """
    section = copy.deepcopy(cfg.get(section_name, {}))
    override = section.pop("override", {})

    # -----------------------------------------
    # Base structure
    # -----------------------------------------
    out = {
        "seed": defaults.get("seed"),
        "dates": copy.deepcopy(defaults.get("dates", {})),
        "paths": copy.deepcopy(defaults.get("paths", {}))
    }

    # -----------------------------------------
    # Merge module-level fields (non override)
    # -----------------------------------------
    for key, val in section.items():
        if key not in ("dates", "paths"):  # reserved keys
            out[key] = val

    # -----------------------------------------
    # SPECIAL: PROMOTIONS DATE RANGES
    # -----------------------------------------
    if section_name == "promotions":
        date_ranges = section.get("date_ranges", [])
        if date_ranges:
            out["date_ranges"] = date_ranges
        else:
            out["date_ranges"] = [{
                "start": out["dates"]["start"],
                "end": out["dates"]["end"]
            }]

    # -----------------------------------------
    # Apply override: dates
    # -----------------------------------------
    if "dates" in override and isinstance(override["dates"], dict):
        out["dates"] = {
            **out["dates"],
            **override["dates"]
        }

    # -----------------------------------------
    # Apply override: seed
    # -----------------------------------------
    if override.get("seed") is not None:
        out["seed"] = override["seed"]

    # -----------------------------------------
    # Apply override: paths
    # -----------------------------------------
    if "paths" in override and isinstance(override["paths"], dict):
        out["paths"] = {
            **out["paths"],
            **override["paths"]
        }

    return out
