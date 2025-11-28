import os
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.logging_utils import info, skip, done


def merge_parquet_files(parquet_files, merged_file, delete_after=False):
    """
    Merge parquet chunk files into a single output parquet.
    Memory-efficient: streams + concatenates incrementally.
    """

    # Filter only existing files
    parquet_files = [f for f in parquet_files if os.path.exists(f)]

    if not parquet_files:
        skip("No parquet chunk files to merge")
        return None

    # Sort for determinism (consistent ordering)
    parquet_files = sorted(parquet_files)

    info(f"Merging {len(parquet_files)} chunks → {os.path.basename(merged_file)}...")

    table = None

    for path in parquet_files:
        chunk = pq.read_table(path)
        table = chunk if table is None else pa.concat_tables([table, chunk])

    # Write merged parquet
    pq.write_table(table, merged_file)

    # Delete individual chunk files if enabled
    if delete_after:
        for path in parquet_files:
            try:
                os.remove(path)
            except Exception:
                pass

    done(f"Merged chunks → {os.path.basename(merged_file)}")

    return merged_file
