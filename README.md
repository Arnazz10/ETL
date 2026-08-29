# Netflix Analytics ETL Pipeline

A Python-based ETL pipeline for processing Netflix title data, loading analytics-ready tables into a database, and publishing dashboard-ready exports for reporting and a static frontend.

![ETL dashboard](frontend/assets/dashboard.png)

## Overview

The project ingests Netflix title records from a CSV file, applies validation and transformation rules, stores normalized data in SQLite or PostgreSQL, and generates analytical outputs for dashboards.

Core capabilities:

- CSV extraction with schema validation
- data cleaning and feature engineering with pandas
- data quality checks for required fields and unique identifiers
- incremental loading based on `show_id`
- normalized database tables for titles, genres, and countries
- analytical SQL query outputs
- staged raw and clean data exports
- Prefect orchestration entrypoint
- static dashboard frontend for Vercel deployment
- unit tests for transformation logic

## Architecture

```mermaid
flowchart LR
    A[Netflix CSV Dataset] --> B[Extract]
    B --> C[Validate Required Columns]
    C --> D[Transform and Clean]
    D --> E[Stage Raw and Clean CSVs]
    D --> F[Load Database Tables]
    F --> G[(SQLite or PostgreSQL)]
    G --> H[Run SQL Analytics]
    H --> I[Analytics CSVs]
    H --> J[Dashboard Exports]
    J --> K[Static Frontend on Vercel]
    F --> L[Logs and Run Summary]
```

## Data Flow

### Extract

Source file:

```text
data/netflix_titles.csv
```

The extract step reads the CSV into a pandas dataframe and validates that the source contains the required columns:

- `show_id`
- `type`
- `title`
- `country`
- `date_added`
- `release_year`
- `rating`
- `listed_in`

Implementation: `etl/extract.py`

### Transform

The transform step prepares the dataset for analytics:

- removes records missing `title` or `type`
- fills missing ratings with `NR`
- parses genres from `listed_in`
- derives `date_added_year`
- normalizes the country field to a primary country
- derives release decade
- assigns a `title_id` surrogate key
- validates non-null mandatory fields
- validates unique `show_id` values

Implementation: `etl/transform.py`

### Load

The load step writes the processed data into the configured database and creates export files.

Database tables:

| Table | Description |
| --- | --- |
| `titles` | One row per Netflix title |
| `genres` | Exploded genre records keyed by title |
| `countries` | Normalized country records keyed by title |
| `staging_titles` | Latest transformed dataset snapshot |

The loader checks existing `show_id` values to support incremental appends. Reporting tables are rebuilt after each run to keep analytics outputs consistent.

Implementation: `etl/load.py`

### Analytics

The pipeline runs SQL queries for:

- top 10 genres
- content added per year
- movies vs TV shows ratio
- top 10 countries
- rating distribution

## Inputs And Outputs

### Inputs

| Input | Location | Purpose |
| --- | --- | --- |
| Netflix title CSV | `data/netflix_titles.csv` | Raw source dataset |
| Environment config | `.env` | Runtime paths and database configuration |

### Outputs

| Output | Location | Purpose |
| --- | --- | --- |
| SQLite database | `output/analytics.db` | Local analytics database |
| Raw extract | `output/staged/raw/raw_extract.csv` | Audit copy of source data |
| Clean titles | `output/staged/clean/cleaned_titles.csv` | Transformed dataset |
| Decade partitions | `output/staged/clean/decade=*/titles.csv` | Partitioned clean data |
| Analytics CSVs | `output/*.csv` | SQL query results |
| Dashboard CSVs | `output/dashboard/*.csv` | Static dashboard data source |
| Database table exports | `output/db_tables/*.csv` | CSV copies of loaded tables |
| Pipeline log | `output/pipeline.log` | Execution log |
| Run summary | `output/run_summary.txt` | Latest run metadata |

## Project Structure

```text
netflix_etl/
├── data/
│   └── netflix_titles.csv
├── dashboards/
│   └── README.md
├── etl/
│   ├── config.py
│   ├── extract.py
│   ├── load.py
│   ├── logging_utils.py
│   ├── quality.py
│   └── transform.py
├── frontend/
│   ├── assets/
│   ├── public/data/
│   ├── app.js
│   ├── index.html
│   └── styles.css
├── orchestration/
│   └── prefect_flow.py
├── output/
│   ├── analytics.db
│   ├── dashboard/
│   ├── db_tables/
│   ├── staged/
│   ├── pipeline.log
│   └── run_summary.txt
├── scripts/
│   └── sync_frontend_data.sh
├── tests/
│   └── test_transform.py
├── .env.example
├── main.py
├── README.md
└── requirements.txt
```

## Setup

```bash
cd netflix_etl
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Configuration

The pipeline reads configuration from `.env`.

| Variable | Purpose | Default |
| --- | --- | --- |
| `NETFLIX_DATA_PATH` | Source CSV path | `data/netflix_titles.csv` |
| `NETFLIX_OUTPUT_DIR` | Output directory | `output` |
| `NETFLIX_LOG_FILE` | Log file path | `output/pipeline.log` |
| `NETFLIX_DATABASE_URL` | SQLAlchemy database URL | SQLite database in `output` |

PostgreSQL example:

```bash
NETFLIX_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/netflix_analytics
```

## Run Pipeline

```bash
source .venv/bin/activate
python main.py
```

## Run With Prefect

```bash
source .venv/bin/activate
python orchestration/prefect_flow.py
```

## Run Tests

```bash
source .venv/bin/activate
pytest
```

## Frontend

The `frontend/` directory contains a static dashboard that reads generated CSV files from `frontend/public/data/`.

Features:

- KPI cards
- type, rating, decade, and title search filters
- bar charts for yearly additions, content type mix, genres, countries, and ratings
- filtered title table

Refresh frontend data after running the ETL pipeline:

```bash
bash scripts/sync_frontend_data.sh
```

Run the frontend locally:

```bash
python3 -m http.server 8000 --directory frontend
```

Open:

```text
http://127.0.0.1:8000
```

## Vercel Deployment

Deploy the static dashboard as a Vercel project:

1. Import the GitHub repository.
2. Set the Vercel root directory to `frontend`.
3. Leave the build command empty.
4. Deploy.

The Python ETL pipeline should run outside Vercel, then publish refreshed CSV files to `frontend/public/data/` before deployment.

## Development Notes

- SQLite is the default database for local runs.
- PostgreSQL is supported through `NETFLIX_DATABASE_URL`.
- `show_id` is used as the natural key for incremental loading.
- `title_id` is generated as a surrogate key for normalized reporting tables.
- Generated output files are useful for auditing and dashboard consumption.
- Do not commit credentials, database passwords, or access tokens.
