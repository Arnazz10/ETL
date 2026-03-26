"""Data-quality helpers for the Netflix ETL pipeline."""

from __future__ import annotations

import logging

import pandas as pd

LOGGER = logging.getLogger(__name__)


def validate_required_columns(dataframe: pd.DataFrame, required_columns: list[str], dataframe_name: str) -> None:
    """Raise an error when expected columns are missing from a dataframe."""
    missing_columns = [column for column in required_columns if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"{dataframe_name} is missing required columns: {missing_columns}")


def validate_no_nulls(dataframe: pd.DataFrame, columns: list[str], dataframe_name: str) -> None:
    """Raise an error when mandatory columns contain null values."""
    null_counts = dataframe[columns].isnull().sum()
    bad_columns = null_counts[null_counts > 0]
    if not bad_columns.empty:
        raise ValueError(f"{dataframe_name} contains nulls in required columns: {bad_columns.to_dict()}")


def validate_unique_values(dataframe: pd.DataFrame, column: str, dataframe_name: str) -> None:
    """Raise an error when a column expected to be unique contains duplicates."""
    duplicate_count = dataframe[column].duplicated().sum()
    if duplicate_count:
        raise ValueError(f"{dataframe_name} contains {duplicate_count} duplicate values in {column}")


def log_data_quality_report(dataframe: pd.DataFrame, dataframe_name: str) -> None:
    """Log a compact data-quality report for operational visibility."""
    LOGGER.info(
        "Data quality report for %s | rows=%s | cols=%s | nulls=%s",
        dataframe_name,
        len(dataframe),
        len(dataframe.columns),
        dataframe.isnull().sum().to_dict(),
    )
