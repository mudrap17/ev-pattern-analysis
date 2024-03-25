import awswrangler as wr
import pandas as pd
import urllib.parse
import os

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        
        # Creating DF from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
        
        # Normalize "Connections" data and associate with "LocationID"
        connections_data = []
        rem_list = ['ConnectionType', 'StatusType','Level','CurrentType']
        for index, row in df_raw.iterrows():
            location_id = row['ID']
            connections = row['Connections']
            location_title = row['AddressInfo']['Title']
            latitude = row['AddressInfo']['Latitude']
            longitude = row['AddressInfo']['Longitude']
            for connection in connections:
                connection['LocationID'] = location_id
                connection['LocationTitle'] = location_title
                connection['Latitude'] = latitude
                connection['Longitude'] = longitude
                connections_data.append(connection)
                
        

       # Convert to DataFrame
        df_connections = pd.DataFrame(connections_data)
        df_connections.drop(['ConnectionType', 'StatusType', 'Level', 'CurrentType'], axis=1, inplace=True)
        print(df_connections.shape)
        
        
        
        wr_response = wr.s3.to_parquet(
            df=df_connections,
            path=f's3://{os_input_s3_cleansed_layer}',
            dataset=True,
            mode=os_input_write_data_operation
        )
        return wr_response
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
       
