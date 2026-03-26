"""Configuration loading for the Netflix ETL pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class PipelineConfig:
    """Store the runtime configuration for the ETL pipeline."""

    project_root: Path
    data_path: Path
    output_dir: Path
    log_file: Path
    database_url: str


def load_config() -> PipelineConfig:
    """Load runtime configuration from environment variables with project-local defaults."""
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    data_path = Path(os.getenv("NETFLIX_DATA_PATH", project_root / "data" / "netflix_titles.csv"))
    output_dir = Path(os.getenv("NETFLIX_OUTPUT_DIR", project_root / "output"))
    log_file = Path(os.getenv("NETFLIX_LOG_FILE", output_dir / "pipeline.log"))
    database_url = os.getenv("NETFLIX_DATABASE_URL", f"sqlite:///{output_dir / 'analytics.db'}")

    return PipelineConfig(
        project_root=project_root,
        data_path=data_path,
        output_dir=output_dir,
        log_file=log_file,
        database_url=database_url,
    )
