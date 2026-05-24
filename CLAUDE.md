# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

F1 Lake is a Formula 1 data engineering project that collects historical F1 data using the [FastF1](https://docs.fastf1.dev/) Python library and stores it as Parquet files. The project follows a lakehouse architecture pipeline: raw collection → Bronze (Delta) → Silver (Feature Store) → Gold (BI/reports) → ML model training (MLFlow) → user application. Currently, only the data collection stage is implemented in this repo.

## Environment Setup

```bash
conda create --name f1-lake python=3.13
conda activate f1-lake
```

Dependencies (no requirements file exists): `fastf1`, `pandas`.

## Running Data Collection

```bash
# Default: collects 2021-2022, Race sessions only
python collect.py

# Custom years and session modes (R=Race, Q=Qualifying, S=Sprint)
python collect.py --years 2023 2024 2025 --modes R Q S
python collect.py -y 2023 -m R Q
```

## Architecture

The codebase is a single module (`collect.py`) containing the `Collect` class:

- **`Collect(year, modes)`** — orchestrator that iterates over years and GP numbers (1-49), collecting session data via FastF1 and saving to `data/` as Parquet files.
- **Collection stops** for a given year when a Race session (`mode='R'`) returns no data for a GP number, indicating no more GPs exist for that year.
- **5-second delay** between years to avoid API rate limits.
- Data is saved using the naming convention `data/{YEAR}_{GP:02d}_{MODE}.parquet`.

## Key Dependencies

- **fastf1** — the F1 data source API; uses `fastf1.get_session()` and the private `_load_drivers_results()` method to fetch driver results.
- **pandas** — all data is handled as DataFrames and saved as Parquet.

## Notes

- The project language (comments, print messages, README) is Portuguese.
- No tests, linting, or CI/CD are configured.
- `.gitignore` excludes `*.parquet` but the `data/` directory contains tracked parquet files from prior commits.
