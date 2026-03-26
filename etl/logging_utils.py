"""Logging utilities for the Netflix ETL pipeline."""

from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(log_file: Path) -> None:
    """Configure console and file logging for the pipeline runtime."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        ],
        force=True,
    )
