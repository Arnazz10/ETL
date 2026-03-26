"""Unit tests for the transform layer."""

from __future__ import annotations

import pandas as pd

from etl.transform import derive_decade, extract_added_year, normalize_country, parse_genres, transform_data


def build_raw_dataframe() -> pd.DataFrame:
    """Create a compact raw dataframe fixture for transform testing."""
    return pd.DataFrame(
        [
            {
                "show_id": "s1",
                "type": "Movie",
                "title": "Example Film",
                "director": "Director A",
                "cast": "Actor A",
                "country": "United States, Canada",
                "date_added": "September 9, 2021",
                "release_year": 2021,
                "rating": None,
                "duration": "95 min",
                "listed_in": "Action & Adventure, Dramas",
                "description": "Sample description",
            },
            {
                "show_id": "s2",
                "type": None,
                "title": "Bad Row",
                "director": "Director B",
                "cast": "Actor B",
                "country": "India",
                "date_added": "July 15, 2020",
                "release_year": 2020,
                "rating": "TV-14",
                "duration": "1 Season",
                "listed_in": "International TV Shows, Romantic TV Shows",
                "description": "Bad row",
            },
        ]
    )


def test_parse_genres_splits_and_trims_values() -> None:
    """Verify genre parsing strips whitespace and drops empty tokens."""
    assert parse_genres(" Comedies, Dramas , ") == ["Comedies", "Dramas"]


def test_extract_added_year_returns_nullable_year() -> None:
    """Verify the added-year helper extracts the correct year from a valid date string."""
    assert extract_added_year("September 9, 2021") == 2021


def test_normalize_country_keeps_first_country_only() -> None:
    """Verify country normalization keeps only the first listed country."""
    assert normalize_country("United States, Canada") == "United States"


def test_derive_decade_maps_year_to_decade_label() -> None:
    """Verify release years map to their expected decade labels."""
    assert derive_decade(2021) == "2020s"


def test_transform_data_cleans_and_enriches_rows() -> None:
    """Verify the main transform step drops invalid rows and adds engineered columns."""
    transformed = transform_data(build_raw_dataframe())
    assert len(transformed) == 1
    assert transformed.loc[0, "rating"] == "NR"
    assert transformed.loc[0, "country_primary"] == "United States"
    assert transformed.loc[0, "date_added_year"] == 2021
    assert transformed.loc[0, "decade"] == "2020s"
    assert transformed.loc[0, "genres"] == ["Action & Adventure", "Dramas"]
