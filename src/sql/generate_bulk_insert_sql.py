import os
from datetime import datetime
from pathlib import Path
from src.utils.logging_utils import info, work, skip


def generate_bulk_insert_script(
    csv_folder,
    table_name=None,
    output_sql_file="bulk_insert.sql",
    field_terminator=",",
    row_terminator="\n",
    codepage="65001",
):
    """
    Generate a BULK INSERT SQL script for all CSV files in a folder.
    """

    csv_folder = Path(csv_folder)

    # Prevent stray script in project root
    if output_sql_file == "bulk_insert.sql":
        output_sql_file = str(csv_folder / "_ignored_bulk_insert.sql")

    # Collect CSV Files
    csv_files = sorted(
        f for f in os.listdir(csv_folder)
        if f.lower().endswith(".csv")
    )

    if not csv_files:
        skip(f"No CSV files found in {csv_folder}. Skipping BULK INSERT script.")
        return None

    # Script header
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "-- Auto-generated BULK INSERT script",
        f"-- Generated on: {timestamp}",
        ""
    ]

    # Build BULK INSERT statements
    for csv_file in csv_files:

        inferred_table = table_name or os.path.splitext(csv_file)[0].capitalize()
        csv_full_path = os.path.abspath(os.path.join(csv_folder, csv_file))

        stmt = f"""
BULK INSERT {inferred_table}
FROM '{csv_full_path}'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = '{field_terminator}',
    ROWTERMINATOR = '{row_terminator}',
    CODEPAGE = '{codepage}',
    TABLOCK
);
"""
        lines.append(stmt.strip())

    # Write final SQL script
    with open(output_sql_file, "w", encoding="utf-8") as out:
        out.write("\n\n".join(lines))

    work(f"Wrote BULK INSERT script → {Path(output_sql_file).name}")
    return output_sql_file
