import boto3
import awswrangler as wr
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        # Extract the database and table name from the key
        key_parts = key.split('/')
        if len(key_parts) < 3:
            continue

        db_name = key_parts[1]  # Assuming the second part is the database name
        table_name = key_parts[2]  # Third part is the table name
        s3_path = f"s3://{bucket}/{'/'.join(key_parts[:3])}/"

        current_databases = wr.catalog.databases()
        if db_name not in [db['DatabaseName'] for db in current_databases['Databases']]:
            wr.catalog.create_database(db_name)

        # Check if the table exists, create or update it
        if not wr.catalog.does_table_exist(database=db_name, table=table_name):
            wr.catalog.create_parquet_table(
                database=db_name,
                path=s3_path,
                table=table_name
                # No partition columns are specified here
            ) 
