# Netflix Analytics ETL Pipeline

An interview-ready data engineering project that extracts Netflix title data from CSV, cleans and validates it with Python, loads it into an analytics database, and exports dashboard-ready datasets for BI tools or a future web frontend.

Repository owner details:

- GitHub username: `Arnazz10`
- Repository: `https://github.com/Arnazz10/ETL`

Do not commit passwords, tokens, or personal access tokens into this repository. Use GitHub authentication locally or Vercel environment variables for secrets.

## Project Goal

The goal is to show a complete mini data platform:

1. Ingest raw Netflix title data.
2. Validate the dataset shape and required columns.
3. Clean and enrich the records for analytics.
4. Load normalized tables into SQLite by default, with PostgreSQL support.
5. Run SQL analytics queries.
6. Export clean CSV files for dashboards, BI tools, or a Vercel frontend.

This is a strong portfolio project because it demonstrates Python ETL, data quality checks, incremental loading, SQL analytics, orchestration, testing, and dashboard outputs.

## MVP

The MVP is the smallest complete version that proves the system works end to end.

- Input: `data/netflix_titles.csv`
- Processing: extract, transform, validate, load, analyze
- Storage: SQLite database at `output/analytics.db`
- Outputs:
  - normalized database tables
  - staged raw and clean CSV files
  - analytics CSVs
  - dashboard-ready CSVs
  - pipeline logs
- Validation: unit tests for transformation logic
- Optional orchestration: Prefect flow

## Architecture

```mermaid
flowchart LR
    A[Netflix CSV Dataset] --> B[Extract Layer]
    B --> C[Data Quality Checks]
    C --> D[Transform Layer]
    D --> E[Staged Raw and Clean CSVs]
    D --> F[Load Layer]
    F --> G[(SQLite or PostgreSQL)]
    G --> H[SQL Analytics Queries]
    H --> I[Analytics CSV Outputs]
    H --> J[Dashboard CSV Exports]
    J --> K[Vercel Frontend MVP]
    F --> L[Pipeline Logs and Run Summary]
```

## How The Pipeline Works

### 1. Extract

File: `etl/extract.py`

The pipeline reads `data/netflix_titles.csv` into a pandas dataframe. It checks that the required columns exist before allowing processing to continue.

Required input columns:

- `show_id`
- `type`
- `title`
- `country`
- `date_added`
- `release_year`
- `rating`
- `listed_in`

### 2. Transform

File: `etl/transform.py`

The transform layer cleans and enriches the raw dataset:

- drops rows missing mandatory `title` or `type`
- fills missing `rating` values with `NR`
- splits `listed_in` into a list of genres
- extracts `date_added_year` from `date_added`
- normalizes `country` to the first listed country
- derives `decade` from `release_year`
- creates a surrogate `title_id`
- validates that `show_id`, `title`, and `type` are not null
- validates that `show_id` is unique

### 3. Load

File: `etl/load.py`

The load layer writes the transformed data into a database and exports files for analytics.

Database tables:

- `titles`: one row per Netflix title
- `genres`: one row per title and genre
- `countries`: one row per title and normalized country
- `staging_titles`: latest cleaned dataset snapshot

The loader supports incremental behavior by checking existing `show_id` values. New titles are appended, and reporting tables are rebuilt from the current title set.

### 4. Analytics

The pipeline runs these SQL reports:

- top 10 genres
- content added per year
- movies vs TV shows ratio
- top 10 countries
- rating distribution

## Inputs And Outputs

### Input

| Input | Location | Purpose |
| --- | --- | --- |
| Netflix title CSV | `data/netflix_titles.csv` | Raw source dataset |
| Environment config | `.env` | Optional paths and database URL |

### Main Outputs

| Output | Location | Purpose |
| --- | --- | --- |
| SQLite database | `output/analytics.db` | Analytics storage |
| Raw staged data | `output/staged/raw/raw_extract.csv` | Audit copy of extracted data |
| Clean staged data | `output/staged/clean/cleaned_titles.csv` | Cleaned analytics dataset |
| Decade partitions | `output/staged/clean/decade=*/titles.csv` | Partitioned clean data |
| Analytics CSVs | `output/*.csv` | SQL query results |
| Dashboard CSVs | `output/dashboard/*.csv` | Frontend or BI-ready files |
| DB table exports | `output/db_tables/*.csv` | Easy inspection of database tables |
| Pipeline log | `output/pipeline.log` | Operational logs |
| Run summary | `output/run_summary.txt` | Latest run summary |

## Current Project Structure

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
├── orchestration/
│   └── prefect_flow.py
├── output/
│   ├── analytics.db
│   ├── dashboard/
│   ├── db_tables/
│   ├── staged/
│   ├── pipeline.log
│   └── run_summary.txt
├── tests/
│   └── test_transform.py
├── .env.example
├── main.py
├── README.md
└── requirements.txt
```

## Local Setup

```bash
cd netflix_etl
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run The ETL Pipeline

```bash
source .venv/bin/activate
python main.py
```

Default database target:

```text
sqlite:///output/analytics.db
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

## Environment Variables

The project reads these optional variables from `.env`:

| Variable | Purpose |
| --- | --- |
| `NETFLIX_DATA_PATH` | Path to source CSV |
| `NETFLIX_OUTPUT_DIR` | Directory for generated outputs |
| `NETFLIX_LOG_FILE` | Pipeline log file path |
| `NETFLIX_DATABASE_URL` | SQLAlchemy database URL |

Example PostgreSQL target:

```bash
NETFLIX_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/netflix_analytics
```

## Vercel Frontend Plan

Vercel is best for the frontend layer, not for running the full Python ETL job as a long-running data pipeline. The practical architecture is:

```mermaid
flowchart LR
    A[Python ETL Run Locally or Scheduled] --> B[Generated Dashboard CSVs]
    B --> C[Commit or Upload Static JSON/CSV]
    C --> D[Next.js Frontend on Vercel]
    D --> E[Interactive Charts and KPI Cards]
```

Recommended frontend MVP:

- build a `frontend/` app using Next.js
- read the exported dashboard CSV files or converted JSON files
- show KPI cards:
  - total titles
  - movie count
  - TV show count
  - top country
  - most common rating
- show charts:
  - movies vs TV shows
  - top 10 genres
  - content added per year
  - rating distribution
  - top 10 countries
- add filters:
  - content type
  - rating
  - country
  - release decade

Good Vercel deployment flow:

1. Run the Python ETL pipeline.
2. Export dashboard CSV/JSON files into the frontend `public/data/` folder.
3. Deploy the Next.js frontend to Vercel.
4. Keep the ETL job separate. Later, schedule it with GitHub Actions, Prefect Cloud, Render cron, or a small VM.

## Included Static Frontend

This repository now includes a dependency-free frontend in `frontend/`. It reads CSV files from `frontend/public/data/` and renders KPI cards, bar charts, filters, and a title table.

Run it locally:

```bash
python3 -m http.server 8000 --directory frontend
```

Then open:

```text
http://127.0.0.1:8000
```

Deploy it on Vercel:

1. Import `https://github.com/Arnazz10/ETL`.
2. Set the Vercel root directory to `frontend`.
3. Leave build command empty.
4. Deploy.

Refresh frontend data after each ETL run:

```bash
bash scripts/sync_frontend_data.sh
```

## Suggested Repo Revamp Roadmap

### Phase 1: Polish Current Backend

- Keep the current ETL code.
- Improve README and diagrams.
- Add sample outputs.
- Add more tests for load and quality validation.
- Add GitHub Actions to run tests on every push.

### Phase 2: Add Frontend

- Create `frontend/` with Next.js.
- Use charts with Recharts or Chart.js.
- Load data from `public/data/*.json`.
- Deploy only the frontend folder to Vercel.

### Phase 3: Make It Production-Like

- Convert CSV exports to JSON for frontend performance.
- Add a scheduled ETL workflow.
- Store production data in PostgreSQL.
- Add data quality failure reporting.
- Add a small API layer only if dynamic querying is needed.

## Interview Explanation

Use this explanation:

> This project is a Netflix analytics ETL pipeline. It extracts raw CSV data, validates the schema, transforms the dataset into analytics-friendly columns, loads normalized tables into SQLite or PostgreSQL, and generates SQL-based reporting outputs. I added staged raw and clean layers for auditability, data quality checks for reliability, incremental loading using `show_id`, and dashboard-ready exports so the same data can power BI dashboards or a Vercel frontend.

Key technical points to mention:

- `pandas` handles extraction and transformation.
- SQLAlchemy abstracts SQLite and PostgreSQL loading.
- `show_id` is the natural unique key used for incremental loading.
- `title_id` is a generated surrogate key for normalized reporting tables.
- Data quality checks fail fast if required columns, nulls, or duplicates break the contract.
- The output layer supports both technical inspection and dashboard consumption.
- Prefect is included to demonstrate orchestration.
- Tests validate important transformation behavior.

## What To Demo

For an interview, run these commands:

```bash
source .venv/bin/activate
python main.py
pytest
```

Then show:

- `output/run_summary.txt`
- `output/dashboard/titles_dashboard.csv`
- `output/top_10_genres.csv`
- `output/analytics.db`
- `etl/transform.py`
- `etl/load.py`
- the architecture diagram in this README

## Future Improvements

- Add a Next.js dashboard for Vercel deployment.
- Add GitHub Actions CI.
- Add JSON exports for frontend consumption.
- Add more load-layer tests with a temporary SQLite database.
- Add Great Expectations or Soda for richer data quality checks.
- Add Docker for reproducible local execution.
