import airflowlib.emr_lib as emr
import os, boto3, logging, zipfile, textwrap
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('transform_movielens_prep', concurrency=3, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# Create a zip file of the lambda function and upload it to S3
def create_and_upload_lambda_zip():
    lambda_file_name = Variable.get("lambda_file_name", "/root/airflow/dags/lambda_scripts/lambda_function.py")
    zip_file_name = Variable.get("zip_file_name", "/root/airflow/dags/lambda_scripts/lambda_function.zip")
    upload_destination_path = Variable.get("upload_destination_path", "lambda_scripts/")

    lambda_code = textwrap.dedent("""\
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
    """)

    try:
        # Write the Lambda function code to a file
        with open(lambda_file_name, 'w') as file:
            file.write(lambda_code)

        # Zip the file
        with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(lambda_file_name, arcname=os.path.basename(lambda_file_name))

        base_zip_file_name = os.path.basename(zip_file_name)
        # Upload the zip file to S3
        s3_client = boto3.client('s3')

        # Upload the zip file to S3 with the dynamic destination path
        s3_client.upload_file(zip_file_name, "kai-airflow-storage", os.path.join(upload_destination_path, base_zip_file_name))

        # Clean up local files
        # os.remove(lambda_file_name)
        # os.remove(zip_file_name)
    except Exception as e:
        logging.error(f"Error in create_and_upload_lambda_zip: {e}")
        raise

def create_lambda_function():
    function_name = Variable.get("lambda_function_name", "CatalogParquetFiles")
    role_arn = Variable.get("lambda_role_arn", "arn:aws:iam::028210085659:role/DataEngLambdaS3CWGlueRole")
    s3_bucket_name = "kai-airflow-storage"
    zip_file_name = Variable.get("zip_file_name", "lambda_scripts/lambda_function.zip")
    s3_zip_file_path = Variable.get("upload_destination_path", "lambda_scripts/") + zip_file_name.split('/')[-1]

    logging.info(f"s3_zip_file_path: {s3_zip_file_path}")

    lambda_client = boto3.client('lambda', region_name=region)

    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.8',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={
                'S3Bucket': s3_bucket_name,
                'S3Key': s3_zip_file_path
            },
            Description='Lambda function for cataloging Parquet files',
            Timeout=600,  # Maximum allowable timeout
            MemorySize=128
        )
        logging.info(f"Lambda function {function_name} created successfully: {response}")
    except Exception as e:
        logging.error(f"Error creating Lambda function {function_name}: {e}")
        raise

    # Add permission for S3 to invoke this Lambda function
    lambda_client.add_permission(
        FunctionName=function_name,
        StatementId='s3invoke',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn='arn:aws:s3:::kai-airflow-storage',
        SourceAccount='028210085659'
    )

    logging.info(f"Permission added to Lambda function {function_name} to allow S3 invocation")


# Define the individual tasks using Python Operators

# Create a PythonOperator to create and upload the Lambda function zip file
create_zip_operator = PythonOperator(
    task_id='create_and_upload_lambda_zip',
    python_callable=create_and_upload_lambda_zip,
    dag=dag
)

create_lambda_function_operator = PythonOperator(
    task_id='create_lambda_function',
    python_callable=create_lambda_function,
    dag=dag
) 

# construct the DAG by setting the dependencies  
create_zip_operator >> create_lambda_function_operator 
