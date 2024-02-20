import pandas as pd
import boto3
import json
import os
from io import BytesIO
from sqlalchemy import create_engine
import logging


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

pd.set_option("display.max_rows", 40)
pd.set_option("display.max_columns", 100)

# Redshift Configuration
redshift_port = '5439'
redshift_database = 'dev'
redshift_schema = 'orion_edw'
table_name = 'program_id_data'
secret_name = 'dev/Redshift/thg'

# S3 Configuration
s3_bucket = 'i3pl-json-silver-dev'
s3_file_path = 'salesforce/'
loaded_folder = 'LOADED/'  # Common 'loaded' folder

# Function to truncate values exceeding the specified length
def truncate_string(value, max_length):
    return value[:max_length] if value is not None else value

# Function to obtain Redshift credentials from AWS Secrets Manager
def obtain_credentials(secrets_client, secret_name):
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary'].decode('utf-8')

        return json.loads(secret)
    except Exception as e:
        logger.error(f"Error obtaining Redshift credentials: {str(e)}")
        raise


# Function to read JSON files from S3 into a Pandas DataFrame in chunks
def read_json_from_s3_in_chunks(s3_client, s3_bucket, s3_file_path, loaded_folder, chunk_size):
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_file_path)

        df_main = pd.DataFrame()

        for obj in response.get('Contents', []):
            json_key = obj['Key']

            if json_key.startswith(loaded_folder):
                continue

            try:
                response = s3_client.get_object(Bucket=s3_bucket, Key=json_key)
                json_data = json.load(response['Body'])

                if not json_data:
                    logger.warning(f"Empty JSON content for file {json_key}. Skipping.")
                    continue

                df = pd.json_normalize(json_data)

                for column in df.columns:
                    if df[column].dtype == 'O':
                        df[column] = df[column].apply(lambda x: truncate_string(x, 256))

                df_main = pd.concat([df_main, df], ignore_index=True)

                # Check if the chunk size is reached
                if len(df_main) >= chunk_size:
                    yield df_main
                    df_main = pd.DataFrame()

            except json.JSONDecodeError as json_err:
                logger.error(f"Error decoding JSON for file {json_key}: {str(json_err)}")
                continue

        # Yield the remaining data if not reaching the chunk size
        if not df_main.empty:
            yield df_main

    except Exception as e:
        logger.error(f"Error reading the JSON files from S3: {str(e)}")
        raise

# Function to write a DataFrame chunk to Redshift
def write_chunk_to_redshift(engine, df_chunk, table_name, redshift_schema):
    try:
        # Write DataFrame chunk to Redshift
        df_chunk.to_sql(name=table_name, con=engine, schema=redshift_schema, if_exists='append', index=False)
        logger.info(f"Successfully wrote DataFrame chunk to Redshift. Rows: {len(df_chunk)}")
    except Exception as e:
        logger.error(f"Error writing DataFrame chunk to Redshift: {str(e)}")
        raise


# Function to move all loaded files to the 'LOADED' folder
def move_files_to_loaded_folder(s3_client, s3_bucket, s3_file_path, loaded_folder):
    try:
        loaded_folder_key = f"{s3_file_path}{loaded_folder}"

        if not s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=loaded_folder_key):
            s3_client.put_object(Bucket=s3_bucket, Key=loaded_folder_key)

        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_file_path)
        for obj in response.get('Contents', []):
            json_key = obj['Key']

            if json_key.startswith(loaded_folder) or json_key.endswith('/'):  # Skip already loaded files or subfolders
                continue

            new_s3_file_path = f"{loaded_folder_key}{os.path.basename(json_key)}"
            s3_client.copy_object(Bucket=s3_bucket, CopySource={'Bucket': s3_bucket, 'Key': json_key}, Key=new_s3_file_path)
            s3_client.delete_object(Bucket=s3_bucket, Key=json_key)

            logger.info(f"Moved file to '{loaded_folder_key}' folder. New path: {new_s3_file_path}")

    except Exception as e:
        logger.error(f"Error moving files to the '{loaded_folder_key}' folder: {str(e)}")
        raise


# Main execution of the script
def main():
    try:
        s3_client = boto3.client('s3')

        logger.info("Reading JSON files from the Salesforce folder in S3.")
        
        # Set the chunk size to 10,000
        chunk_size = 10000
        for df_chunk in read_json_from_s3_in_chunks(s3_client, s3_bucket, s3_file_path, loaded_folder, chunk_size):
            if not df_chunk.empty:
                logger.info("Obtaining Redshift credentials.")
                db_credentials = obtain_credentials(boto3.client('secretsmanager', region_name='us-west-2'), secret_name)
                logger.info(f"db_creds: {db_credentials}")

                connection_string = f"postgresql+psycopg2://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{redshift_port}/{redshift_database}"
                engine = create_engine(connection_string)

                # Write DataFrame chunk to Redshift
                write_chunk_to_redshift(engine, df_chunk, table_name, redshift_schema)

        # Move all loaded files to the 'LOADED' folder after processing all chunks
        move_files_to_loaded_folder(s3_client, s3_bucket, s3_file_path, loaded_folder)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
