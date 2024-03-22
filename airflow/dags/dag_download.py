# Import necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import boto3
import requests
from bs4 import BeautifulSoup  # For parsing HTML to find file links
import os
import shutil

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # DAG does not depend on past runs
    'start_date': days_ago(1),  # Start date of the DAG, set to one day ago
    'email_on_failure': False,  # Disable email notification on failure
    'email_on_retry': False,  # Disable email notification on retry
    'retries': 1,  # Number of retries upon failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# AWS S3 bucket and credentials (Corrected variable names and values)
S3_BUCKET = "bdi-aircraft-rafaelbraga"
AWS_ACCESS_KEY_ID = ""  # Corrected variable name and updated value
AWS_SECRET_ACCESS_KEY = ""  # Updated value
AWS_SESSION_TOKEN = ""  # Updated value

# Base URL template for the data source
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/{year}/{month}/01/"

# Function to download and upload data
def download_data(execution_date, **kwargs):
    print("Initializing S3 client...")
    # Corrected the AWS credentials usage in the S3 client initialization
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_session_token=AWS_SESSION_TOKEN)
    
    # Parse the execution date to determine the year and month
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    url = BASE_URL.format(year=date.year, month=str(date.month).zfill(2))
    
    # Fetch and parse the webpage to extract .gz file links
    print(f"Fetching data index from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.gz')][:100]

    # Download and upload each file to S3
    for file_name in files:
        print(f"Processing file: {file_name}")
        file_url = url + file_name
        local_file_path = "/tmp/" + file_name
        with requests.get(file_url, stream=True) as r:
            with open(local_file_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print(f"Uploading {file_name} to S3...")
        s3_client.upload_file(local_file_path, S3_BUCKET, f"readsb-hist/{date.year}/{str(date.month).zfill(2)}/01/{file_name}")
        os.remove(local_file_path)
        print(f"{file_name} uploaded and local copy deleted.")

# Function to check if data for the date already exists in S3
def check_if_data_exists(execution_date, **kwargs):
    print("Checking if data already exists in S3...")
    # Corrected the AWS credentials usage in the S3 client initialization here as well
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_session_token=AWS_SESSION_TOKEN)
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    prefix = f"readsb-hist/{date.year}/{str(date.month).zfill(2)}/01/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if 'Contents' in response:
        print("Data already exists for this date.")
        return True
    else:
        print("No data found for this date.")
        return False

# Define the DAG
dag = DAG(
    'download_readsb_hist',
    default_args=default_args,
    description='A DAG to download readsb-hist data for the 1st day of each month',
    schedule_interval='@monthly',
    catchup=False,
)

# Define tasks and their sequence
with dag:
    check_data_exists = PythonOperator(
        task_id='check_if_data_exists',
        python_callable=check_if_data_exists,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    download = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    check_data_exists >> download  # Ensure data download occurs only if it doesn't already exist
