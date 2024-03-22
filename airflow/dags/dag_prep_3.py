from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import logging
import json
import gzip

# Assuming you've already defined these variables in your DAG or globally
DB_HOST = 'host.docker.internal'
DB_PORT = 5433
DB_NAME = 'postgres'
DB_PASSWORD = 'postgres'
DB_USERNAME = 'postgres'
directory_path = "/opt/airflow/dags/data/aircraft_type"


def create_tables():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    c = conn.cursor()
    logging.info(c)
    
    # Create aircraft_basic_info table
    c.execute('''CREATE TABLE IF NOT EXISTS aircraft_basic_info
                 (icao TEXT PRIMARY KEY, reg TEXT, icaotype TEXT, year TEXT, 
                 manufacturer TEXT, model TEXT, ownop TEXT, faa_pia BOOLEAN, faa_ladd BOOLEAN, 
                 short_type TEXT, mil BOOLEAN)''')
    
    # Create fuel_consumption table
    c.execute('''CREATE TABLE IF NOT EXISTS fuel_consumption
                 (icao TEXT PRIMARY KEY, name TEXT, galph INTEGER, category TEXT, source TEXT)''')
    
    conn.commit()
    conn.close()

def insert_data():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    c = conn.cursor()

    # Insert data into aircraft_basic_info
    with gzip.open('/opt/airflow/dags/data/aircraft_type/basic-ac-db.json.gz', 'rt', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            print(data)
            c.execute('''INSERT INTO aircraft_basic_info 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                         ON CONFLICT (icao) DO NOTHING''', 
                      (data['icao'], data['reg'], data['icaotype'], data['year'], 
                       data['manufacturer'], data['model'], data['ownop'], 
                       data['faa_pia'], data['faa_ladd'], data['short_type'], data['mil']))

    # Insert data into fuel_consumption
    with open(f'{directory_path}/aircraft_type_fuel_consumption_rates.json') as f:
        fuel_data = json.load(f)
        for icao, info in fuel_data.items():
            c.execute('''INSERT INTO fuel_consumption 
                         VALUES (%s, %s, %s, %s, %s)
                         ON CONFLICT (icao) DO NOTHING''', 
                      (icao, info['name'], info['galph'], info['category'], info['source']))

    conn.commit()
    conn.close()

dag = DAG(
    'aircraft_data_insertion',
    default_args={'start_date': datetime(2024, 3, 20)},
    description='DAG to insert aircraft data into PostgreSQL',
    schedule_interval=None,
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

create_tables_task >> insert_data_task
