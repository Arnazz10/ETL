"""Extraction logic for the Netflix ETL pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from etl.quality import validate_required_columns

LOGGER = logging.getLogger(__name__)


def extract_data(csv_path: str) -> pd.DataFrame:
    """Read the Netflix titles CSV, log dataset profiling details, and return the raw dataframe."""
    resolved_path = Path(csv_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Input dataset not found at {resolved_path}")

    dataframe = pd.read_csv(resolved_path)
    validate_required_columns(
        dataframe,
        required_columns=[
            "show_id",
            "type",
            "title",
            "country",
            "date_added",
            "release_year",
            "rating",
            "listed_in",
        ],
        dataframe_name="raw_extract",
    )

    LOGGER.info("Extracted dataframe shape: %s", dataframe.shape)
    LOGGER.info("Column names: %s", list(dataframe.columns))
    LOGGER.info("Null counts:\n%s", dataframe.isnull().sum())

    return dataframe
