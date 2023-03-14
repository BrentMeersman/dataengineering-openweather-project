# Import libraries

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
from azure.storage.filedatalake import DataLakeFileClient

# Extract weather data from OpenWeather API

def extract_weather_data():
    api_key = '8328839ad6dd82815f586e37f8b50064'
    city = 'Aalst,be'
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    response = requests.get(url)
    data = response.json()

    # Write data to CSV file
    with open('weather_data.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['city', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_degrees', 'cloudiness'])
        writer.writerow([city, data['main']['temp'], data['main']['humidity'], data['main']['pressure'], data['wind']['speed'], data['wind']['deg'], data['clouds']['all']])

    # Upload the CSV file to Azure Data Lake
    storage_account_name = 'datamanproject1'
    file_system_name = 'raw'
    client_id = '9668f20a-85cb-4f77-8f86-0844e25e9b74'
    client_secret = 'hTa8Q~6LtWPlMLpvYE1IGwB50X228ZXsNbraYbIk'
    tenant_id = '4726a35c-3923-4db5-b2cc-9c0b70ffb3d6'
    dl_credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    service_client = DataLakeFileClient.from_connection_string(conn_str=f'DefaultEndpointsProtocol=https;AccountName={storage_account_name};FileSystem={file_system_name};', credential=dl_credential)
    with open('weather_data.csv', mode='rb') as file:
        service_client.upload_file('weather_data.csv', file)


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

extract_data_task