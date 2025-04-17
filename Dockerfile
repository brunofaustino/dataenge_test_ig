FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create data directory with correct permissions
RUN mkdir -p /data/processed && \
    chown -R airflow:root /data

# Switch to airflow user
USER airflow

# Copy requirements first to leverage Docker cache
COPY --chown=airflow:root requirements.txt .
COPY --chown=airflow:root requirements-airflow.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-airflow.txt

# Create Airflow directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Set environment variables
ENV PYTHONPATH=/opt/airflow
ENV PYTHONUNBUFFERED=1

# Default command will be inherited from the base image 