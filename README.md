# Contoso Fake Data Generator – README

A fast, configurable data generator that produces a full retail analytics dataset:

- **Dimension tables** (Customers, Stores, Dates, Geography, Promotions, Currency, Exchange Rates)  
- **Sales Fact table** (millions+ rows, chunked, parquet/CSV/Delta)  
- **SQL bulk insert scripts & CREATE TABLE scripts**  
- **Smart versioning** — dimensions regenerate only when their config changes  
- **Modular pipeline architecture** for easy extension  

## Features

### Modular ETL-like pipeline  
The generator runs in three stages:  
1. **Dimensions pipeline**  
2. **Sales fact pipeline**  
3. **Packaging + SQL + final output**  

### Supports multiple output formats  
- CSV  
- Parquet  
- Delta Parquet  

### Efficient chunked generation  
Handles millions of sales rows without large memory usage.

### Auto-packaged output  
Final output folder includes:

```
facts/
dims/
bulk_insert_*.sql
create_tables.sql
```

### Smart versioning  
Dimensions only regenerate when config changes — saves time on large datasets.

## Project Architecture

```
├── docs/
│   └── assets/
├── main.py
├── samples/
└── src/
    ├── cli.py
    ├── dimensions/
    │   ├── __init__.py
    │   ├── currency.py
    │   ├── customers.py
    │   ├── dates.py
    │   ├── exchange_rates.py
    │   ├── geography.py
    │   ├── promotions.py
    │   └── stores.py
    ├── engine/
    │   ├── __init__.py
    │   ├── config/
    │   │   ├── __init__.py
    │   │   ├── config.py
    │   │   └── config_loader.py
    │   ├── dimension_loader.py
    │   ├── packaging.py
    │   └── runners/
    │       ├── __init__.py
    │       ├── dimensions_runner.py
    │       └── sales_runner.py
    ├── facts/
    │   ├── __init__.py
    │   └── sales/
    │       ├── __init__.py
    │       ├── sales.py
    │       ├── sales_logic/
    │       │   ├── __init__.py
    │       │   ├── chunk_builder.py
    │       │   ├── date_logic.py
    │       │   ├── globals.py
    │       │   ├── order_logic.py
    │       │   ├── price_logic.py
    │       │   └── promo_logic.py
    │       ├── sales_worker.py
    │       └── sales_writer.py
    ├── integrations/
    │   ├── __init__.py
    │   └── fx_yahoo.py
    ├── tools/
    │   └── sql/
    │       ├── __init__.py
    │       ├── generate_bulk_insert_sql.py
    │       ├── generate_create_table_scripts.py
    │       └── project_tree.py
    ├── utils/
    │   ├── __init__.py
    │   ├── logging_utils.py
    │   ├── output_utils.py
    │   └── static_schemas.py
    └── versioning/
        ├── __init__.py
        ├── version_checker.py
        └── version_store.py
```

## How the Pipeline Works

### 1. Load & validate config  
Handled by `pipeline/config.py`.

### 2. Generate dimensions  
Orchestrated by `pipeline/dimensions.py`.

### 3. Generate Sales Fact  
Handled by `pipeline/sales_pipeline.py`.

### 4. Package final output  
Handled by `pipeline/packaging.py`.

## How to Run

```bash
python main.py
```

## Configuration (config.json)

Example:

```json
# Final output location for packaged datasets
final_output_folder: "./generated_datasets"

# ---------------------------------------------------------------------
# DEFAULTS (used unless overridden in each section)
# ---------------------------------------------------------------------
defaults:
  seed: 42

  dates:
    start: "2020-01-03"
    end: "2025-10-18"

  paths:
    geography: "./data/parquet_dims/geography.parquet"


# ---------------------------------------------------------------------
# SALES FACT GENERATION
# ---------------------------------------------------------------------
sales:
  parquet_folder: "./data/parquet_dims"
  out_folder: "./data/fact_out"

  total_rows: 1000000
  chunk_size: 1000000

  file_format: "deltaparquet"          # csv | parquet | deltaparquet
  write_delta: true
  delta_output_folder: "./data/fact_out/delta"

  merge_parquet: true
  merged_file: "sales.parquet"
  delete_chunks: true

  row_group_size: 1000000
  compression: "snappy"
```

## Extending the Project

Add new dimensions or facts by simply dropping new modules into the correct folder and updating the pipeline modules accordingly.
