from datetime import date

PRESETS = {
    "Small (Demo)": {
        "start": date(2023, 1, 1),
        "end": date(2023, 3, 31),
        "rows": 100_000,
    },
    "Medium": {
        "start": date(2022, 1, 1),
        "end": date(2023, 12, 31),
        "rows": 2_000_000,
    },
    "Large": {
        "start": date(2020, 1, 1),
        "end": date(2024, 12, 31),
        "rows": 10_000_000,
    },
}


def apply_preset(cfg, base_loader, preset_name: str):
    """
    Reset config using a named preset.
    """
    preset = PRESETS[preset_name]

    cfg.clear()
    cfg.update(base_loader())

    cfg["defaults"]["dates"]["start"] = preset["start"]
    cfg["defaults"]["dates"]["end"] = preset["end"]
    cfg["sales"]["total_rows"] = preset["rows"]
