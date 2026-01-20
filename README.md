# Zoomcamp Docker SQL

A data pipeline project for loading NYC taxi trip data into PostgreSQL, built as part of the DataTalks Club Data Engineering Zoomcamp.

## Overview

This project downloads NYC Yellow Taxi trip data from the DataTalks Club repository and ingests it into a PostgreSQL database in chunks using pandas and SQLAlchemy.

## Features

- Downloads and processes NYC taxi data from compressed CSV files
- Handles large datasets efficiently using chunked reading (100k rows per chunk)
- Proper data type handling for optimal PostgreSQL storage
- Progress tracking with tqdm
- Containerized with Docker for easy deployment

## Prerequisites

- Python 3.13+
- PostgreSQL database
- Docker (optional, for containerized deployment)

## Setup

### Local Development

1. Install dependencies using uv:
```bash
uv sync
```

2. Set up PostgreSQL database:
```bash
# Using Docker
docker run -it \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=root \
  -e POSTGRES_DB=ny_taxi \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

### Docker Deployment

Build the Docker image:
```bash
docker build -t zoomcamp-docker-sql .
```

## Usage

### Data Ingestion Script

The main ingestion script (`ingest_data.py`) downloads NYC taxi data and loads it into PostgreSQL:

```bash
python ingest_data.py
```

The script:
- Downloads data from: `https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz`
- Connects to PostgreSQL at `localhost:5432` (database: `ny_taxi`)
- Creates/replaces the `yellow_taxi_data` table
- Processes data in 100k row chunks with progress bar

### Pipeline Script

Run the example pipeline:
```bash
python pipeline.py <day>
```

Example:
```bash
python pipeline.py 10
```

### Using Docker

Run the containerized pipeline:
```bash
docker run zoomcamp-docker-sql 10
```

## Configuration

Database connection parameters in `ingest_data.py`:
```python
db_user = "root"
db_password = "root"
db_host = "localhost"
db_port = 5432
db_name = "ny_taxi"
```

Data source configuration:
```python
year = "2021"
month = "01"
```

## Data Schema

The `yellow_taxi_data` table includes:
- VendorID (Int64)
- Passenger count (Int64)
- Trip distance (float64)
- Pickup/dropoff timestamps (datetime)
- Location IDs (Int64)
- Payment information (float64)
- And more...

## Development

The project includes:
- **ingest_data.py**: Main data ingestion script
- **pipeline.py**: Example pipeline for Docker demonstration
- **notebook.ipynb**: Jupyter notebook for exploratory analysis
- **Dockerfile**: Multi-stage build using uv for fast dependency installation

### Dependencies

Production:
- pandas
- sqlalchemy
- psycopg2-binary
- pyarrow
- tqdm

Development:
- jupyter
- pgcli