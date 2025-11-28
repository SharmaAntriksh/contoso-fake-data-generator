# sales_writer.py
import os
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.logging_utils import info, skip, done


def merge_parquet_files(parquet_files, merged_file, delete_after=False):
    """
    Merge a list of parquet chunk files into a final parquet file.
    Expects parquet_files to be a list of full file paths.
    """
    # filter out missing files (safety)
    parquet_files = [f for f in parquet_files if os.path.exists(f)]

    if not parquet_files:
        skip("No parquet chunk files to merge")
        return None

    info(f"Merging {len(parquet_files)} chunks → {os.path.basename(merged_file)}...")

    # merge progressively (lower memory footprint than reading all at once)
    table = None
    for f in parquet_files:
        chunk = pq.read_table(f)
        table = chunk if table is None else pa.concat_tables([table, chunk])

    # write merged file
    pq.write_table(table, merged_file)

    # optionally delete chunks
    if delete_after:
        for f in parquet_files:
            try:
                os.remove(f)
            except Exception:
                pass

    done(f"Merged chunks → {os.path.basename(merged_file)}")

    return merged_file
