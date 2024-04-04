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
    response = requests.get(f'https://api.openchargemap.io/v3/poi/?output=json&countrycode=US&boundingbox={bounding_box}&maxresults=100&key=yourapikey&operationalstatus={city}')
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
        
# Define DAG schedule for snowflake integration
snowflake_dag = DAG(
    's3_to_snowflake',
    default_args=default_args,
    description='Transfer data from S3 to Snowflake with email notification',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1),
    tags=['s3', 'snowflake'],
)

# Define S3 sensor
s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_name='your_s3_bucket',
    prefix='your_s3_prefix',
    aws_conn_id='aws_default',
    dag=snowflake_dag,
)

# Define Snowflake query to create table
create_table_sql = """
CREATE TABLE ev_table
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@S3_stage',
          FILE_FORMAT=>'s3_file_format'
        )
      ));
"""

# Define Snowflake operator to create table
create_table = SnowflakeOperator(
    task_id='create_table',
    sql=create_table_sql,
    snowflake_conn_id='snowflake_default',
    dag=snowflake_dag,
)

# Define Snowflake query to copy data
copy_data_sql = """
COPY INTO ev_table from @S3_stage
  file_format = 's3_file_format'
  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
"""

# Define Snowflake operator to copy data
copy_data = SnowflakeOperator(
    task_id='copy_data',
    sql=copy_data_sql,
    snowflake_conn_id='snowflake_default',
    dag=snowflake_dag,
)

# Define email notification task
email_notification = EmailOperator(
    task_id='email_notification',
    to='your@email.com',
    subject='Table Creation and Data Copy Completed',
    html_content='The table creation and data copy operations in Snowflake have been successfully completed.',
    dag=snowflake_dag,
)

# Define task dependencies
create_table >> copy_data >> email_notification


