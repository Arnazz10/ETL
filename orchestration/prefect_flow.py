"""Prefect orchestration entrypoint for the Netflix ETL pipeline."""

from __future__ import annotations

from prefect import flow, task

from etl.config import load_config
from main import run_pipeline


@task(name="load_config")
def load_pipeline_config():
    """Load the pipeline configuration for Prefect execution."""
    return load_config()


@task(name="run_netflix_etl")
def run_netflix_etl(config):
    """Execute the main ETL workflow under Prefect orchestration."""
    return run_pipeline(config)


@flow(name="netflix-etl-flow", log_prints=True)
def netflix_etl_flow():
    """Run the Netflix ETL workflow as a Prefect flow."""
    config = load_pipeline_config()
    return run_netflix_etl(config)


if __name__ == "__main__":
    netflix_etl_flow()
