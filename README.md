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
src/
├── pipeline/
│   ├── config.py
│   ├── dimensions.py
│   ├── sales_pipeline.py
│   ├── packaging.py
│
├── dimensions/
│   ├── customers.py
│   ├── promotions.py
│   ├── stores.py
│   ├── dates.py
│   ├── currency.py
│   ├── exchange_rates.py
│   ├── geography_builder.py
│
├── facts/
│   ├── sales.py
│
├── sql/
│   ├── generate_bulk_insert_sql.py
│   ├── generate_create_table_scripts.py
│
├── utils/
│   ├── output_utils.py
│   ├── versioning.py
│   ├── static_schemas.py
│
└── main.py
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
{
  "sales": {
    "total_rows": 5000000,
    "chunk_size": 250000,
    "file_format": "parquet",
    "parquet_folder": "output/parquet_dims",
    "out_folder": "output/facts",
    "merge_parquet": true,
    "write_delta": false
  },
  "customers": {
    "geography_source": {
      "path": "data/DimGeography.parquet",
      "max_geos": 10000
    }
  }
}
```

## Extending the Project

Add new dimensions or facts by simply dropping new modules into the correct folder and updating the pipeline modules accordingly.
