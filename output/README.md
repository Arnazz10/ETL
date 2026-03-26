# Output Artifacts

This folder contains the generated artifacts produced by the Netflix ETL pipeline run.

## What is here

- `analytics.db`: SQLite database containing the loaded pipeline tables
- `top_10_genres.csv`: analytical SQL output for the most common genres
- `content_added_per_year.csv`: analytical SQL output showing yearly content additions
- `movies_vs_tv_ratio.csv`: analytical SQL output comparing Movies vs TV Shows
- `top_10_countries.csv`: analytical SQL output for top countries by content count
- `rating_distribution.csv`: analytical SQL output for rating mix
- `run_summary.txt`: compact text summary of the latest ETL execution
- `pipeline.log`: runtime log showing extraction, transformation, validation, and load activity

## Subfolders

- `db_tables/`: CSV snapshots of the core database tables for quick inspection
- `dashboard/`: dashboard-ready CSV exports for Power BI or Tableau
- `staged/raw/`: raw extracted dataset snapshot
- `staged/clean/`: cleaned dataset snapshot and decade-based partitions

## Why this helps

This structure makes the repository easier to review because a recruiter or interviewer can quickly inspect:

- the loaded database artifact
- the SQL analytics outputs
- the underlying loaded tables
- the staged raw and transformed data layers
- the operational run log
