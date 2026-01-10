import os


def cpu_count_safe():
    return os.cpu_count() or 1


def validate(cfg):
    """
    Returns (errors, warnings)
    """
    errors = []
    warnings = []

    dates = cfg["defaults"]["dates"]
    sales = cfg["sales"]

    if not dates.get("start") or not dates.get("end"):
        errors.append("Start date and end date must be set.")
    elif dates["end"] < dates["start"]:
        errors.append("End date must be after start date.")

    if sales["total_rows"] <= 0:
        errors.append("Total rows must be greater than zero.")

    if sales["chunk_size"] > sales["total_rows"]:
        warnings.append("Chunk size exceeds total rows.")

    if sales["file_format"] == "csv" and sales["total_rows"] > 5_000_000:
        warnings.append("Large CSV outputs can be slow and very large.")

    if sales["workers"] > cpu_count_safe():
        warnings.append("Workers exceed CPU cores.")

    return errors, warnings
