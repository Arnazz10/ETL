"""Entry point for the Netflix ETL pipeline."""

from __future__ import annotations

import logging
from datetime import datetime

from etl.config import PipelineConfig, load_config
from etl.extract import extract_data
from etl.load import load_data
from etl.logging_utils import configure_logging
from etl.quality import log_data_quality_report
from etl.transform import transform_data

LOGGER = logging.getLogger(__name__)


def timestamp_message(step_name: str) -> None:
    """Log a timestamped status line for the current ETL step."""
    LOGGER.info("[%s] %s", datetime.now().isoformat(timespec="seconds"), step_name)


def run_pipeline(config: PipelineConfig) -> dict[str, object]:
    """Run the extract, transform, and load stages in sequence and return summary metadata."""
    timestamp_message("Starting extract step")
    raw_dataframe = extract_data(str(config.data_path))

    timestamp_message("Starting transform step")
    cleaned_dataframe = transform_data(raw_dataframe)
    log_data_quality_report(cleaned_dataframe, "cleaned_titles")

    timestamp_message("Starting load step")
    load_metadata = load_data(
        raw_dataframe=raw_dataframe,
        cleaned_dataframe=cleaned_dataframe,
        database_url=config.database_url,
        output_dir=str(config.output_dir),
    )

    timestamp_message("Pipeline complete")
    summary = {
        "raw_row_count": len(raw_dataframe),
        "cleaned_row_count": len(cleaned_dataframe),
        "titles_loaded": load_metadata["titles_loaded"],
        "new_titles_loaded": load_metadata["new_titles_loaded"],
        "genres_loaded": load_metadata["genres_loaded"],
        "countries_loaded": load_metadata["countries_loaded"],
        "csv_outputs_created": len(load_metadata["query_results"]),
        "database_url": load_metadata["database_url"],
    }

    LOGGER.info("Final summary stats:")
    for key, value in summary.items():
        LOGGER.info("%s: %s", key, value)

    return summary


def main() -> None:
    """Load configuration, configure logging, and execute the ETL pipeline."""
    config = load_config()
    configure_logging(config.log_file)

    try:
        run_pipeline(config)
    except Exception:
        LOGGER.exception("Pipeline execution failed.")
        raise


if __name__ == "__main__":
    main()
