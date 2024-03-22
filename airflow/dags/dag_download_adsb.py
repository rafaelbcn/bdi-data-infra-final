import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Function to download the .gz file
def download_adsb_exchange_db(**kwargs):
    url = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
    # The directory within the dags directory where the file will be saved
    directory = "/opt/airflow/dags/data/aircraft_type"
    filename = "basic-ac-db.json.gz"
    target_path = os.path.join(directory, filename)

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Check if the file already exists to ensure idempotency
    if not os.path.exists(target_path):
        response = requests.get(url, stream=True)
        response.raise_for_status()  # This will raise an HTTPError if the download fails
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("File downloaded successfully.")
    else:
        print("File already exists. No download needed.")

    return target_path

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),  # Adjust start_date to a suitable value
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='download_adsb_exchange_db',
    default_args=default_args,
    description='A DAG to download ADS-B Exchange database',
    schedule_interval=None,  # None means this DAG will only be triggered manually
    catchup=False
)

# Define the PythonOperator to run the function
download_task = PythonOperator(
    task_id='download_adsb_exchange_db_task',
    python_callable=download_adsb_exchange_db,
    dag=dag,
)

# Set the order of the tasks in the DAG
download_task
