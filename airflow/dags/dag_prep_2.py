from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import boto3
import json
import pandas as pd
import logging
import gzip

# DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 20),
    'retries': 1
}

dag = DAG(
    'aircraft_data_processing',
    default_args=default_args,
    description='A simple DAG to process aircraft data',
    schedule_interval=None,  # Set to None if you want to trigger the DAG manually
)

# Database configuration variables
DB_HOST = 'localhost'
DB_PORT = 5433
DB_NAME = 'postgres'
DB_PASSWORD = 'postgres'
DB_USERNAME = 'postgres'

# Hardcoded AWS S3 credentials
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_SESSION_TOKEN = ''
S3_BUCKET_NAME = 'bdi-aircraft-rafaelbraga'

def format_for_db(aircraft: dict) -> tuple:
    """
    Format aircraft data for database insertion.
    
    Parameters:
    - aircraft: A dictionary containing aircraft data.
    
    Returns:
    - A tuple formatted for database insertion, ensuring that the data types
      are consistent with the database schema.
    """
    try:
        altitude_baro = int(aircraft.get("alt_baro", 0))  # Try to convert 'alt_baro' to integer, default to 0
    except ValueError:
        altitude_baro = 0  # Default to 0 if conversion fails

    emergency_str = aircraft.get("emergency", "").lower()  # Get emergency status and convert to lowercase
    had_emergency = emergency_str in ['true', 't', '1', 'yes']  # Determine boolean value for emergency status

    # Prepare and return the tuple formatted for database insertion
    return (
        aircraft.get("hex", ""),  # ICAO aircraft address
        aircraft.get("r", ""),  # Registration
        aircraft.get("t", ""),  # Aircraft type
        altitude_baro,  # Barometric altitude
        float(aircraft.get("gs", 0.0)),  # Ground speed, default to 0.0
        had_emergency,  # Emergency status
        pd.to_datetime(aircraft.get("timestamp")),  # Timestamp converted to datetime object
        float(aircraft.get("lat", 0.0)),  # Latitude, default to 0.0
        float(aircraft.get("lon", 0.0)),  # Longitude, default to 0.0
        int(aircraft.get("messages", 0)),  # Number of ADS-B messages
        float(aircraft.get("now", 0.0))  # Current time, default to 0.0
    )
def download_and_prepare_data():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )

    logger.info("Starting data download...")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
    all_files = [obj['Key'] for obj in response['Contents']] if 'Contents' in response else []

    formatted_data_list = []

    for file in all_files:
        if file.endswith('.json.gz'):
            obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file)
            with gzip.GzipFile(fileobj=obj['Body']) as gzipfile:
                file_content = gzipfile.read().decode('utf-8')
            data_dict = json.loads(file_content)
            formatted_data = [format_for_db(ac) for ac in data_dict.get('aircraft', [])]
            formatted_data_list.extend(formatted_data)

    logger.info("Data download and preparation completed successfully.")
    return formatted_data_list

def initialize_database_and_store_data(formatted_data):
    """
    Initializes the database by creating a table for aircraft data if it doesn't already exist,
    and then stores the formatted aircraft data in the database.

    Parameters:
    - formatted_data: A list of tuples representing formatted aircraft data.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS aircraft_data (
            icao VARCHAR(255) UNIQUE,
            registration VARCHAR(255),
            aircraft_type VARCHAR(255),
            altitude_baro INTEGER,
            ground_speed DOUBLE PRECISION,
            had_emergency BOOLEAN,
            timestamp TIMESTAMP,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            messages BIGINT,
            now DOUBLE PRECISION
        )
    ''')
    conn.commit()

    if formatted_data:
        query = """
        INSERT INTO aircraft_data (icao, registration, aircraft_type, altitude_baro, ground_speed, had_emergency, timestamp, lat, lon, messages, now)
        VALUES %s ON CONFLICT (icao) DO NOTHING;
        """
        execute_values(cursor, query, formatted_data, template=None, page_size=100)
        conn.commit()

    cursor.close()
    conn.close()
    print("Database initialization and data storage completed successfully.")


def task_download_and_prepare_data(**kwargs):
    formatted_data = download_and_prepare_data()
    kwargs['ti'].xcom_push(key='formatted_data', value=formatted_data)

download_and_prepare_data_task = PythonOperator(
    task_id='download_and_prepare_data',
    python_callable=task_download_and_prepare_data,
    provide_context=True,
    dag=dag,
)

# Initialize database and store data task
def task_initialize_database_and_store_data(**kwargs):
    ti = kwargs['ti']
    formatted_data = ti.xcom_pull(task_ids='download_and_prepare_data', key='formatted_data')
    if formatted_data:
        initialize_database_and_store_data(formatted_data)

initialize_database_and_store_data_task = PythonOperator(
    task_id='initialize_database_and_store_data',
    python_callable=task_initialize_database_and_store_data,
    provide_context=True,
    dag=dag,
)

# Define DAG order
download_and_prepare_data_task >> initialize_database_and_store_data_task