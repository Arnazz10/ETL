"""Transformation logic for the Netflix ETL pipeline."""

from __future__ import annotations

import logging
from typing import List

import pandas as pd

from etl.quality import validate_no_nulls, validate_unique_values

LOGGER = logging.getLogger(__name__)


def parse_genres(value: str) -> List[str]:
    """Convert a comma-separated genre string into a cleaned list of genres."""
    if pd.isna(value):
        return []
    return [genre.strip() for genre in str(value).split(",") if genre.strip()]


def extract_added_year(value: str) -> object:
    """Extract the calendar year from the date_added column and return a nullable year value."""
    if pd.isna(value):
        return pd.NA

    parsed_date = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed_date):
        return pd.NA

    return int(parsed_date.year)


def normalize_country(value: str) -> str:
    """Reduce the country field to the first listed country or mark it as Unknown when missing."""
    if pd.isna(value):
        return "Unknown"
    return str(value).split(",")[0].strip() or "Unknown"


def derive_decade(release_year: object) -> str:
    """Convert a release year into a decade label such as 2010s or Unknown."""
    if pd.isna(release_year):
        return "Unknown"
    decade_start = int(release_year) // 10 * 10
    return f"{decade_start}s"


def transform_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw dataframe, engineer analytics-friendly columns, and return the transformed dataframe."""
    before_count = len(dataframe)
    cleaned = dataframe.copy()

    cleaned = cleaned.dropna(subset=["title", "type"])
    cleaned["rating"] = cleaned["rating"].fillna("NR")
    cleaned["genres"] = cleaned["listed_in"].apply(parse_genres)
    cleaned["date_added_year"] = cleaned["date_added"].apply(extract_added_year)
    cleaned["country_primary"] = cleaned["country"].apply(normalize_country)
    cleaned["decade"] = cleaned["release_year"].apply(derive_decade)

    cleaned = cleaned.reset_index(drop=True)
    cleaned["title_id"] = cleaned.index + 1

    validate_no_nulls(cleaned, ["show_id", "title", "type"], "transformed_titles")
    validate_unique_values(cleaned, "show_id", "transformed_titles")

    after_count = len(cleaned)
    LOGGER.info("Rows before transform: %s", before_count)
    LOGGER.info("Rows after transform: %s", after_count)

    return cleaned
