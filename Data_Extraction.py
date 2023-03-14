# Import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.macros import macros   
from datetime import datetime, timedelta
import requests
import csv
from azure.storage.filedatalake import DataLakeFileClient
from azure.identity import ClientSecretCredential

# Extract weather data from OpenWeather API
def extract_weather_data():
    api_key = '8328839ad6dd82815f586e37f8b50064'
    city = 'Aalst,be'
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    response = requests.get(url)
    data = response.json()

    # Retrieve the connection information from Airflow
    conn = BaseHook.get_connection('azure_data_lake_conn')

    # Create the DataLakeFileClient
    storage_account_name = conn.login
    file_system_name = 'raw'
    credential = ClientSecretCredential(conn.extra['tenant_id'], conn.extra['client_id'], conn.password)
    service_client = DataLakeFileClient.from_connection_string(conn_str=f'DefaultEndpointsProtocol=https;AccountName={storage_account_name};FileSystem={file_system_name};', credential=credential)

    # Generate the name and path of the CSV file
    filename = f'weather_data_{macros.ds_nodash}.csv'
    filepath = f'/raw/weather/{macros.ds_nodash}/{filename}'

    # Write data to CSV file
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['city', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_degrees', 'cloudiness'])
        writer.writerow([city, data['main']['temp'], data['main']['humidity'], data['main']['pressure'], data['wind']['speed'], data['wind']['deg'], data['clouds']['all']])

    # Upload the CSV file to Azure Data Lake
    with open(filename, mode='rb') as file:
        service_client.upload_file(filepath, file)

# Define the default arguments for the DAG
default_args = {
    'owner': 'Brent Dataman',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG('extract_weather_data', default_args=default_args, schedule_interval='@daily')

# Define the PythonOperator to extract the weather data
extract_data_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag
)