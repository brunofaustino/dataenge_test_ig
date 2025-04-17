# Common Crawl Data Processing

This project processes Common Crawl data to extract and analyze external links from websites.

## Project Structure

```
.
├── data/                      # Data directory
│   ├── raw/                   # Raw WAT files
│   ├── processed/             # Processed data
│   └── final/                 # Final output files
├── dags/                      # Airflow DAGs
│   └── common_crawl_analysis.py
├── src/                       # Source code
│   ├── data_processor.py      # Data processing logic
│   └── website_categorizer.py # Website categorization
├── tests/                     # Test files
├── docker-compose.yml         # Docker configuration
└── requirements.txt           # Python dependencies
```

## Features

1. Downloads and processes WAT files from Common Crawl
2. Extracts external links from web pages
3. Categorizes websites based on content
4. Stores results in PostgreSQL and Parquet format

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start the services:
   ```bash
   docker compose up -d
   ```

3. Access Airflow web interface at http://localhost:8080

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