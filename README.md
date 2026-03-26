# Netflix ETL Pipeline

Production-style Netflix ETL pipeline in Python with staged raw/clean outputs, data-quality checks, incremental loading, SQL analytics, Prefect orchestration, SQLite or PostgreSQL targets, and dashboard-ready exports for Power BI or Tableau.

## Project Structure

```text
netflix_etl/
├── data/
│   └── netflix_titles.csv
├── dashboards/
│   └── README.md
├── etl/
│   ├── __init__.py
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
│   ├── staged/
│   └── pipeline.log
├── tests/
│   └── test_transform.py
├── .env.example
├── main.py
├── README.md
└── requirements.txt
```

## Features

- `logging` to console and `output/pipeline.log`
- `.env`-driven configuration for dataset path, outputs, and database target
- data validation for required columns, null checks, and duplicate keys
- pinned dependencies in `requirements.txt`
- unit tests for transformation logic with `pytest`
- incremental loading based on unseen `show_id` values
- staged `raw` and `clean` output layers
- partitioned clean CSV exports by `decade`
- analytical SQL reporting persisted as CSVs
- dashboard-ready CSVs for Power BI or Tableau
- Prefect flow for orchestration
- SQLite by default, PostgreSQL via environment variable

## Setup

```bash
cd "/home/arnab/CODE/ETL-Data Engineering/netflix_etl"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run With SQLite

```bash
source .venv/bin/activate
python main.py
```

Default SQLite target:

- `sqlite:////home/arnab/CODE/ETL-Data Engineering/netflix_etl/output/analytics.db`

## Run With PostgreSQL

Update `.env`:

```bash
NETFLIX_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/netflix_analytics
```

Then run:

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

## Outputs

- `output/analytics.db` or PostgreSQL target tables
- `output/top_10_genres.csv`
- `output/content_added_per_year.csv`
- `output/movies_vs_tv_ratio.csv`
- `output/top_10_countries.csv`
- `output/rating_distribution.csv`
- `output/staged/raw/raw_extract.csv`
- `output/staged/clean/cleaned_titles.csv`
- `output/staged/clean/decade=*/titles.csv`
- `output/dashboard/*.csv`
- `output/pipeline.log`

## Analytical Queries

1. Top 10 genres by count
2. Content added per year
3. Movies vs TV Shows ratio
4. Top 10 countries by content
5. Rating distribution

## Notes

- The sample dataset allows the project to run without Kaggle access.
- Replacing `data/netflix_titles.csv` with the Kaggle version keeps the same workflow.
- Dashboard source files are documented in `dashboards/README.md`.
