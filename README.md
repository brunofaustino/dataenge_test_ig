# Common Crawl Data Processing Pipeline

This project processes Common Crawl WAT files to extract and analyze website metrics, storing results in PostgreSQL.

## Setup

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

3. Initialize Airflow:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

4. Configure PostgreSQL connection in Airflow:
```bash
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'postgres' \
    --conn-password 'your_password' \
    --conn-port 5432 \
    --conn-schema 'postgres'
```

## Running the Pipeline

1. Start the Airflow webserver:
```bash
airflow webserver --port 8080
```

2. In a new terminal, start the Airflow scheduler:
```bash
airflow scheduler
```

3. Access the Airflow UI at http://localhost:8080 and enable the DAG 'common_crawl_processing'

## Project Structure

- `src/`: Source code for Common Crawl data processing
- `dags/`: Airflow DAG definitions
- `airflow/`: Airflow home directory (created during setup)
- `data/`: Directory for temporary data storage (gitignored)

## Monitoring

Monitor pipeline execution through:
- Airflow UI at http://localhost:8080
- Logs in `airflow/logs/`
- PostgreSQL database for processed metrics

## Features

1. Downloads and processes WAT files from Common Crawl
2. Extracts external links from web pages
3. Categorizes websites based on content
4. Stores results in PostgreSQL and Parquet format

## Data Processing

The pipeline:
1. Downloads WAT files from Common Crawl
2. Extracts external links and metadata
3. Categorizes websites
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

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd data-engineer-challenge
   ```

2. Set up the project:
   ```bash
   make setup
   ```

3. Build and start the services:
   ```bash
   make build
   make up
   ```

4. Initialize Airflow:
   ```bash
   make init-airflow
   ```

## Usage

1. Access the Airflow web interface at `http://localhost:8080`
   - Username: admin
   - Password: admin

2. The pipeline can be triggered manually through the Airflow UI or scheduled to run automatically.

3. To process new Common Crawl segments:
   - Navigate to the DAGs section in Airflow
   - Find the "common_crawl_analysis" DAG
   - Click "Trigger DAG" and provide the segment IDs

## Development

- Format code:
  ```bash
  make format
  ```

- Run tests:
  ```bash
  make test
  ```

- Run linting:
  ```bash
  make lint
  ```

## Data Flow

1. Download Common Crawl segments
2. Extract external links
3. Load data into PostgreSQL
4. Analyze link structure and categorize websites
5. Compute metrics
6. Save results in columnar format

## Metrics

The pipeline computes the following metrics:
1. Total number of unique domains
2. Distribution of website categories
3. Geographic distribution of websites
4. Ad-based vs. non-ad-based domain ratio
5. Average number of subsections per domain

## Checking database data

```sh
docker compose exec postgres psql -U dataengineer -d crawldata -c "SELECT COUNT(*) FROM external_links;"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

# Get Segments

wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/wet.paths.gz

# 
gunzip wet.paths.gz