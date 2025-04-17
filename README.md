# Common Crawl Data Processing Pipeline

This project processes Common Crawl WAT files to extract and analyze website metrics, storing results in PostgreSQL.

## Quick Start

For a completely automated setup, run:

```bash
make setup-airflow
```

This command will:
1. Create the necessary directory structure
2. Build the Docker images
3. Start the services
4. Initialize the Airflow database
5. Create an admin user
6. Set up connections and variables
7. Provide instructions for accessing the Airflow UI

## Manual Setup (Alternative)

If you prefer to set up the project manually:

1. Create a Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
pip install -r requirements-airflow.txt
```

3. Set up the project structure:
```bash
make setup
```

4. Build and start the services:
```bash
make build
make up
```

5. Initialize Airflow:
```bash
make init-airflow
make setup-airflow
```

6. Access the Airflow web interface at `http://localhost:8080`
   - Username: admin
   - Password: admin


7. Running DAGs

Run the DAGs in the following order:

- common_crawl_segment_collector
- common_crawl_processing


![Airflow DAGs](img/airflow_dags.jpg)
![Airflow DAG Crawl Precessing](img/common_crawl_processing.jpg)
![Airflow DAG Segment Colector](img/common_crawl_segment_collector.jpg)


## Project Structure

- `src/`: Source code for Common Crawl data processing
- `dags/`: Airflow DAG definitions
- `airflow/`: Airflow home directory (created during setup)
- `data/`: Directory for temporary data storage (gitignored)



## Features

1. Downloads and processes WAT files from Common Crawl
2. Extracts external links from web pages
3. Categorizes websites based on content
4.Stores results in PostgreSQL and Parquet format

## Data Processing

The pipeline:
1. Downloads WAT files from Common Crawl
2. Extracts external links and metadata
3.Categorizes websites
4. Saves results to PostgreSQL and Parquet

## Development

Run tests:
```bash
python -m pytest tests/
```

## Data Storage

- PostgreSQL: Raw extracted links and metadata
- Parquet: Final processed data, partitioned by source domain

## Requirements

- Python 3.8+
- Apache Airflow 2.7.0+
- PostgreSQL 13+
- Docker and Docker Compose

## Prerequisites

- Docker
- Docker Compose
- Make
- Git

## Project Structure

```
.
├── data/               # Data directory
│   ├── raw/           # Raw Common Crawl data
│   ├── processed/     # Intermediate processed data
│   └── final/         # Final output data
├── src/               # Source code
├── dags/              # Airflow DAGs
├── docker/            # Docker configuration
├── tests/             # Test files
├── notebooks/         # Jupyter notebooks
└── config/            # Configuration files
```

# Cleanup

When you're done working with the project, you can clean up resources using the provided Make targets:

## Basic Cleanup

```bash
# Stop all services
make down

# Clean up data and containers
make clean
```

## Comprehensive Cleanup

For a more thorough cleanup that includes Docker images:

```bash
# Clean up everything (containers, volumes, images, data)
make clean-all
```

## Data-Only Cleanup

If you only want to clean up data files without affecting containers:

```bash
# Clean up only data files
make clean-data
```

## Virtual Environment Cleanup

To remove the Python virtual environment:

```bash
# Remove Python virtual environment
make clean-venv
```
