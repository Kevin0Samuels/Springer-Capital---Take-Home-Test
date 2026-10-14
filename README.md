# Referral Pipeline

This project processes referral, user, transaction, and reward CSV files with PySpark. It joins the source data, evaluates referral business rules, and writes a consolidated CSV report.

## Requirements

- Docker Desktop or Docker Engine
- The seven input CSV files in the `data/` directory

The Docker image is based on `apache/spark:3.5.0-python3`. Python dependencies are installed from [`requirements.txt`](./requirements.txt):

- `pyspark==3.5.0` — data processing
- `pandas==2.0.3` — data profiling utility
- `openpyxl==3.1.2` — Excel output support for the asset-generation utility

You do not need to install Python or these packages on your host to run the pipeline in Docker.

## Input data

Place these files under `data/`:

```text
data/
├── lead_logs.csv
├── paid_transactions.csv
├── referral_rewards.csv
├── user_logs.csv
├── user_referral_logs.csv
├── user_referral_statuses.csv
└── user_referrals.csv
```

Each CSV must include a header row. The pipeline infers column types when reading the files.

## Build and run with Docker

From the project root, build the image:

```sh
docker build -t referral-pipeline .
```

Run the pipeline with the input data mounted read-only and the output directory mounted for persistence:

```sh
mkdir -p output
docker run --rm \
  -v "$PWD/data:/app/data:ro" \
  -v "$PWD/output:/app/output" \
  referral-pipeline
```

The container runs `pipeline.py` automatically. On success, the report is written to:

```text
output/result_data.csv
```

The pipeline writes a single CSV file with a header row and replaces an existing `result_data.csv` at that output location.

## Configuration

The container sets the following defaults:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATA_DIR` | `/app/data` | Directory containing the input CSV files |
| `OUTPUT_FILE` | `/app/output/result_data.csv` | Path for the generated report |

To use different paths inside the container, set these variables and mount the corresponding host directories. For example:

```sh
docker run --rm \
  -e DATA_DIR=/input \
  -e OUTPUT_FILE=/reports/referrals.csv \
  -v "$PWD/data:/input:ro" \
  -v "$PWD/output:/reports" \
  referral-pipeline
```

## Project files

- [`pipeline.py`](./pipeline.py) — main PySpark referral processing job, executed by the container.
- [`profile_data.py`](./profile_data.py) — optional local utility that creates CSV column profiles in `output/profiling/`.
- [`generate_assets.py`](./generate_assets.py) — optional local utility for generating supporting assets.
- [`Dockerfile`](./Dockerfile) — builds the Spark-based runtime image.
