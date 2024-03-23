from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


from datetime import datetime
import requests
import boto3
import json

S3_CONN_ID='s3_conn'
BUCKET='ev-api-data'

from geopy.geocoders import Nominatim

def get_city_bounding_box(city_name):
    geolocator = Nominatim(user_agent="city_bounding_box")
    location = geolocator.geocode(city_name)
    if location:
        return location.raw['boundingbox']
    else:
        return None

def fetch_data_and_upload_to_s3(city):
    coordinates = get_city_bounding_box(city)
    bounding_box = ','.join([f'({float(coordinates[i])},{float(coordinates[i+1])})' for i in range(0, len(coordinates), 2)])
    # Make API request to fetch data
    response = requests.get(f'https://api.openchargemap.io/v3/poi/?output=json&countrycode=US&boundingbox={bounding_box}&maxresults=100&key=1470325c-1ed9-4f43-b32b-e0cb00eaab47&operationalstatus={city}')
    print(response)
    data = response.json()
    print(data)
    s3client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
    s3client.put_object(Bucket=BUCKET, Key=city, Body=json.dumps(data))
    #s3_hook=S3Hook(aws_conn_id=S3_CONN_ID) 
    # Upload data to Amazon S3
    #s3_hook.load_file(city,data, bucket_name=BUCKET, replace=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 3),
    'retries': 1,
}


with DAG('charging_points_processing', schedule_interval='@daily', default_args=default_args) as dag:
    cities = ['Chicago', 'Houston', 'New York', 'San Francisco' ,'Colorado']  # Example cities
    for city in cities:
        fetch_task = PythonOperator(
            task_id=f'fetch_charging_points_{city.replace(" ", "_")}',
            python_callable=fetch_data_and_upload_to_s3,
            op_kwargs={'city': city},
        )
        fetch_task

