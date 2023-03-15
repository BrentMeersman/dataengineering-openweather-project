# Import libraries
from datetime import datetime, timedelta
import requests
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Extract weather data from OpenWeather API
def extract_weather_data():
    api_key = '8328839ad6dd82815f586e37f8b50064'
    city = 'Aalst,be'
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    response = requests.get(url)
    data = response.json()

    # Generate the name of the CSV file
    filename = f'weather_data_{datetime.now().strftime("%Y%m%d")}.csv'

    # Write data to CSV file
    with open(filename, mode='w', newline='') as file:
        fieldnames = ['city', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_degrees', 'cloudiness']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            'city': city,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'wind_degrees': data['wind']['deg'],
            'cloudiness': data['clouds']['all']
        })

    # Upload the CSV file to Azure Blob Storage using SAS key
    account_name = 'datamanproject1'
    container_name = 'raw'
    blob_name = f'weather/{filename}'
    sas_token = 'sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-01-01T16:00:42Z&st=2023-03-15T08:00:42Z&spr=https&sig=mQkuRwZ2LVO8TAQWQ%2BteQ%2BU2%2FwHk7UrwCl6BhFHr1UY%3D'
    blob_url_with_sas = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    blob_client = BlobClient.from_blob_url(blob_url_with_sas)
    with open(filename, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)

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
