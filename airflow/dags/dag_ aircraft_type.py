import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Function to download the JSON file
def download_fuel_consumption_rates(**kwargs):
    # URL to the raw JSON content on GitHub
    url = "https://github.com/martsec/flight_co2_analysis/raw/main/data/aircraft_type_fuel_consumption_rates.json"
    # The directory within the dags directory where the file will be saved
    directory = "/opt/airflow/dags/data/aircraft_type"
    filename = "aircraft_type_fuel_consumption_rates.json"
    target_path = os.path.join(directory, filename)

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Check if the file already exists to ensure idempotency
    if not os.path.exists(target_path):
        response = requests.get(url)
        response.raise_for_status()  # This will raise an HTTPError if the download fails
        with open(target_path, "wb") as f:
            f.write(response.content)
        print("File downloaded successfully.")
    else:
        print("File already exists. No download needed.")

    return target_path

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='download_aircraft_fuel_consumption',
    default_args=default_args,
    description='A DAG to download aircraft fuel consumption rates',
    schedule_interval=None,  # None means this DAG will only be triggered manually
    catchup=False
)

# Define the PythonOperator to run the function
download_task = PythonOperator(
    task_id='download_fuel_consumption_rates',
    python_callable=download_fuel_consumption_rates,
    dag=dag,
)

# Set the order of the tasks in the DAG
download_task
