from airflow.models import Connection, Variable
from airflow.utils.session import create_session
import os

def initialize_connections():
    with create_session() as session:
        # Add PostgreSQL connection
        postgres_conn = Connection(
            conn_id='postgres_default',
            conn_type='postgres',
            host='postgres',  # service name from docker-compose
            schema='postgres',  # database name
            login='postgres',  # username from docker-compose
            password='postgres',  # password from docker-compose
            port=5432
        )
        
        # Check if connection already exists
        if not session.query(Connection).filter(Connection.conn_id == postgres_conn.conn_id).first():
            session.add(postgres_conn)
            print(f"Added connection: {postgres_conn.conn_id}")

def initialize_variables():
    # Common Crawl configuration
    variables = {
        'COMMONCRAWL_BASE_URL': 'https://data.commoncrawl.org',
        'WAT_FILES_PER_BATCH': '2',
        'MAX_DOMAINS_PER_BATCH': '1000',
        'RETENTION_DAYS': '90',
        'PROCESSING_THREADS': '4'
    }
    
    for key, value in variables.items():
        if Variable.get(key, default_var=None) is None:
            Variable.set(key, value)
            print(f"Added variable: {key} = {value}")

if __name__ == "__main__":
    initialize_connections()
    initialize_variables()
    print("Airflow initialization completed successfully") 