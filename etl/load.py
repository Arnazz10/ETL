"""Loading logic for the Netflix ETL pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from etl.quality import validate_no_nulls

LOGGER = logging.getLogger(__name__)


def prepare_titles_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Select and order the transformed title-level columns for the titles table."""
    titles = dataframe[
        [
            "title_id",
            "show_id",
            "type",
            "title",
            "director",
            "cast",
            "country_primary",
            "date_added",
            "date_added_year",
            "release_year",
            "rating",
            "duration",
            "listed_in",
            "description",
            "decade",
        ]
    ].rename(columns={"country_primary": "country"})
    validate_no_nulls(titles, ["show_id", "title", "type", "country"], "titles")
    return titles


def prepare_genres_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Explode the list of genres into a normalized table keyed by title_id."""
    exploded = dataframe[["title_id", "show_id", "genres"]].explode("genres")
    exploded = exploded.dropna(subset=["genres"]).rename(columns={"genres": "genre"})
    return exploded.reset_index(drop=True)


def prepare_countries_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Project the normalized primary country into a lookup-style table keyed by title_id."""
    countries = dataframe[["title_id", "show_id", "country_primary"]].rename(columns={"country_primary": "country"})
    return countries.reset_index(drop=True)


def save_query_result(result: pd.DataFrame, output_dir: Path, file_name: str) -> None:
    """Write an analytical query result dataframe to a CSV file in the output directory."""
    result.to_csv(output_dir / file_name, index=False)


def save_dashboard_exports(results: Dict[str, pd.DataFrame], titles: pd.DataFrame, output_dir: Path) -> None:
    """Persist dashboard-friendly datasets for BI tools such as Power BI or Tableau."""
    dashboard_dir = output_dir / "dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    titles.to_csv(dashboard_dir / "titles_dashboard.csv", index=False)
    for name, dataframe in results.items():
        dataframe.to_csv(dashboard_dir / f"{name}_dashboard.csv", index=False)


def stage_dataframes(raw_dataframe: pd.DataFrame, cleaned_dataframe: pd.DataFrame, output_dir: Path) -> None:
    """Save raw and cleaned pipeline layers to staged output locations for auditability."""
    raw_dir = output_dir / "staged" / "raw"
    clean_dir = output_dir / "staged" / "clean"
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)

    raw_dataframe.to_csv(raw_dir / "raw_extract.csv", index=False)
    cleaned_dataframe.to_csv(clean_dir / "cleaned_titles.csv", index=False)

    for decade, partition in cleaned_dataframe.groupby("decade", dropna=False):
        partition_dir = clean_dir / f"decade={decade}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition.to_csv(partition_dir / "titles.csv", index=False)


def build_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for SQLite or PostgreSQL targets."""
    return create_engine(database_url, future=True)


def fetch_existing_show_ids(engine: Engine) -> set[str]:
    """Read existing show_ids from the target titles table to support incremental loads."""
    query = text("SELECT show_id FROM titles")
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            return {str(row[0]) for row in result}
    except Exception:
        LOGGER.info("No existing titles table found. Running an initial full load.")
        return set()


def supports_incremental_schema(engine: Engine) -> bool:
    """Check whether the existing titles table contains the columns required for incremental rebuilds."""
    try:
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns("titles")}
        return {"show_id", "title_id", "listed_in", "country"}.issubset(columns)
    except Exception:
        return False


def fetch_max_title_id(engine: Engine) -> int:
    """Read the current maximum title_id value from the titles table for incremental key assignment."""
    query = text("SELECT COALESCE(MAX(title_id), 0) FROM titles")
    try:
        with engine.connect() as connection:
            result = connection.execute(query).scalar_one()
            return int(result or 0)
    except Exception:
        return 0


def filter_incremental_records(dataframe: pd.DataFrame, existing_show_ids: set[str]) -> pd.DataFrame:
    """Keep only records whose show_id is not already present in the target system."""
    if not existing_show_ids:
        return dataframe.copy()

    incremental = dataframe[~dataframe["show_id"].astype(str).isin(existing_show_ids)].copy()
    LOGGER.info("Incremental records selected: %s", len(incremental))
    return incremental


def assign_incremental_title_ids(dataframe: pd.DataFrame, starting_title_id: int) -> pd.DataFrame:
    """Assign surrogate title_id values to newly detected records during incremental loads."""
    if dataframe.empty:
        return dataframe.copy()

    reassigned = dataframe.reset_index(drop=True).copy()
    reassigned["title_id"] = reassigned.index + starting_title_id + 1
    return reassigned


def replace_dimension_tables(engine: Engine, titles: pd.DataFrame, genres: pd.DataFrame, countries: pd.DataFrame) -> None:
    """Rebuild the normalized reporting tables from the current titles dataset."""
    titles.to_sql("titles", engine, if_exists="replace", index=False)
    genres.to_sql("genres", engine, if_exists="replace", index=False)
    countries.to_sql("countries", engine, if_exists="replace", index=False)


def append_fact_delta(engine: Engine, titles_delta: pd.DataFrame) -> None:
    """Append only new titles into the persistent titles table during incremental processing."""
    if titles_delta.empty:
        LOGGER.info("No new titles detected for append.")
        return
    titles_delta.to_sql("titles", engine, if_exists="append", index=False)
    LOGGER.info("Appended %s new titles.", len(titles_delta))


def read_full_titles(engine: Engine) -> pd.DataFrame:
    """Read the current titles table from the target database after load completion."""
    return pd.read_sql_query("SELECT * FROM titles", engine)


def rebuild_reporting_tables(engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Regenerate genres and countries tables from the complete titles dataset after incremental loading."""
    titles = read_full_titles(engine)
    genres = titles[["title_id", "show_id", "listed_in"]].copy()
    genres["genre"] = genres["listed_in"].fillna("").str.split(",")
    genres = genres.explode("genre")
    genres["genre"] = genres["genre"].astype(str).str.strip()
    genres = genres[genres["genre"] != ""][["title_id", "show_id", "genre"]].reset_index(drop=True)

    countries = titles[["title_id", "show_id", "country"]].copy()

    genres.to_sql("genres", engine, if_exists="replace", index=False)
    countries.to_sql("countries", engine, if_exists="replace", index=False)
    return titles, genres, countries


def create_staging_table(engine: Engine, dataframe: pd.DataFrame) -> None:
    """Persist the latest transformed dataset into a staging table used for downstream rebuilds and audits."""
    staging = dataframe[
        [
            "show_id",
            "listed_in",
            "country_primary",
            "date_added",
            "date_added_year",
            "release_year",
            "rating",
            "duration",
            "type",
            "title",
            "director",
            "cast",
            "description",
            "decade",
            "title_id",
        ]
    ].rename(columns={"country_primary": "country"})
    staging.to_sql("staging_titles", engine, if_exists="replace", index=False)


def run_analytical_queries(engine: Engine, output_dir: Path) -> Dict[str, pd.DataFrame]:
    """Execute the required analytical SQL queries, log the results, and persist them as CSV files."""
    queries = {
        "top_10_genres": """
            SELECT genre, COUNT(*) AS content_count
            FROM genres
            GROUP BY genre
            ORDER BY content_count DESC, genre ASC
            LIMIT 10
        """,
        "content_added_per_year": """
            SELECT date_added_year AS year_added, COUNT(*) AS content_count
            FROM titles
            WHERE date_added_year IS NOT NULL
            GROUP BY date_added_year
            ORDER BY year_added ASC
        """,
        "movies_vs_tv_ratio": """
            SELECT type, COUNT(*) AS content_count
            FROM titles
            GROUP BY type
            ORDER BY content_count DESC, type ASC
        """,
        "top_10_countries": """
            SELECT country, COUNT(*) AS content_count
            FROM countries
            GROUP BY country
            ORDER BY content_count DESC, country ASC
            LIMIT 10
        """,
        "rating_distribution": """
            SELECT rating, COUNT(*) AS content_count
            FROM titles
            GROUP BY rating
            ORDER BY content_count DESC, rating ASC
        """,
    }

    results: Dict[str, pd.DataFrame] = {}
    for name, query in queries.items():
        result = pd.read_sql_query(query, engine)
        results[name] = result
        LOGGER.info("%s:\n%s", name.replace("_", " ").title(), result)
        save_query_result(result, output_dir, f"{name}.csv")

    return results


def load_data(
    raw_dataframe: pd.DataFrame,
    cleaned_dataframe: pd.DataFrame,
    database_url: str,
    output_dir: str,
) -> Dict[str, object]:
    """Load transformed data into SQLite or PostgreSQL, run analytics, and return operational metadata."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stage_dataframes(raw_dataframe, cleaned_dataframe, output_path)
    engine = build_engine(database_url)
    create_staging_table(engine, cleaned_dataframe)

    titles = prepare_titles_table(cleaned_dataframe)
    existing_show_ids = fetch_existing_show_ids(engine)
    incremental_schema_ready = supports_incremental_schema(engine)
    titles_delta = filter_incremental_records(titles, existing_show_ids)
    titles_delta = assign_incremental_title_ids(titles_delta, fetch_max_title_id(engine))

    if not existing_show_ids or not incremental_schema_ready:
        if existing_show_ids and not incremental_schema_ready:
            LOGGER.info("Existing database schema is outdated. Rebuilding tables with the current schema.")
        genres = prepare_genres_table(cleaned_dataframe)
        countries = prepare_countries_table(cleaned_dataframe)
        replace_dimension_tables(engine, titles, genres, countries)
        loaded_titles = titles
    else:
        append_fact_delta(engine, titles_delta)
        loaded_titles, genres, countries = rebuild_reporting_tables(engine)

    query_results = run_analytical_queries(engine, output_path)
    save_dashboard_exports(query_results, loaded_titles, output_path)

    metadata = {
        "query_results": query_results,
        "titles_loaded": len(loaded_titles),
        "new_titles_loaded": len(titles_delta) if existing_show_ids else len(titles),
        "genres_loaded": len(genres),
        "countries_loaded": len(countries),
        "database_url": database_url,
    }
    return metadata
