import os
import pandas as pd
import boto3
import json
from io import BytesIO
from sqlalchemy import create_engine
from sqlalchemy.types import Float, String, DateTime
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
redshift_database = 'lvi3plp-stg'
redshift_schema = 'orion_edw'
table_name = 'cust_commitment_s'
secret_name = 'uat/Redshift/thg'

# S3 Configuration
s3_bucket = 'i3pl-xml-bronze-uat'
s3_path = 'fixed_commitments/Raw_files/'
loaded_folder = 'loaded/'

# Mapping of Excel column names to Redshift column names
column_name_mapping = {
    'reference-tab name': 'REFERENCE_TAB_NAME',
    'bob naming convention reference': 'BOB_NAMING_CONVENTION_REFERENCE',
    'contract': 'CONTRACT',
    'customer': 'CUSTOMER',
    'hierarchy': 'HIERARCHY',
    'parenth1': 'PARENTH1',
    'site': 'SITE',
    'commitment type': 'COMMITMENT_TYPE',
    'contract start': 'CONTRACT_START',
    'contract end': 'CONTRACT_END',
    'summary year': 'SUMMARY_YEAR',
    '1': 'COL_1',
    'jan 2022': 'JAN_2022',
    'feb 2022': 'FEB_2022',
    'mar 2022': 'MAR_2022',
    'apr 2022': 'APR_2022',
    'may 2022': 'MAY_2022',
    'jun 2022': 'JUN_2022',
    'jul 2022': 'JUL_2022',
    'aug 2022': 'AUG_2022',
    'sep 2022': 'SEP_2022',
    'oct 2022': 'OCT_2022',
    'nov 2022': 'NOV_2022',
    'dec 2022': 'DEC_2022',
    'average 2022': 'AVERAGE_2022',
    'jan 2023': 'JAN_2023',
    'feb 2023': 'FEB_2023',
    'mar 2023': 'MAR_2023',
    'apr 2023': 'APR_2023',
    'may 2023': 'MAY_2023',
    'jun 2023': 'JUN_2023',
    'jul 2023': 'JUL_2023',
    'aug 2023': 'AUG_2023',
    'sep 2023': 'SEP_2023',
    'oct 2023': 'OCT_2023',
    'nov 2023': 'NOV_2023',
    'dec 2023': 'DEC_2023',
    'average 2023': 'AVERAGE_2023',
    'au_created_date': 'AU_CREATED_DATE',
    'au_updated_date': 'AU_UPDATED_DATE'
}

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

# Function to get the latest Excel file from S3
def get_latest_excel_file(s3_client, s3_bucket, s3_path):
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_path)
        
        if 'Contents' in response and response['Contents']:
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            return latest_file['Key']
        else:
            logger.warning("No objects found in the specified S3 path.")
            return None
    except Exception as e:
        logger.error(f"Error fetching the latest Excel file: {str(e)}")
        raise

# Function to read an Excel file from S3 into a Pandas DataFrame
def read_excel_from_s3(s3_client, s3_bucket, s3_file_path):
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_path)
        df = pd.read_excel(BytesIO(response['Body'].read()), skiprows=4)

        # Map Excel column names to Redshift column names
        df.columns = [column_name_mapping.get(str(col).lower(), col) for col in df.columns]

        return df
    except Exception as e:
        logger.error(f"Error reading the Excel file from S3: {str(e)}")
        raise

# Function to write a DataFrame to Redshift
def write_to_redshift(engine, df_main, table_name, redshift_schema):
    try:
        df_main['AU_CREATED_DATE'] = pd.to_datetime('now')
        df_main['AU_UPDATED_DATE'] = pd.to_datetime('now')

        df_main.to_sql(
            name=table_name,
            con=engine,
            schema=redshift_schema,
            if_exists='append',  # Change to 'replace' if needed
            index=False,
            dtype={
                'REFERENCE_TAB_NAME': String(256),
                'BOB_NAMING_CONVENTION_REFERENCE': String(256),
                'CONTRACT': String(256),
                'CUSTOMER': String(256),
                'HIERARCHY': String(256),
                'PARENTH1': String(256),
                'SITE': String(256),
                'COMMITMENT_TYPE': String(256),
                'CONTRACT_START': DateTime(timezone=False),
                'CONTRACT_END': DateTime(timezone=False),
                'SUMMARY_YEAR': String(256),
                'COL_1': Float,
                'JAN_2022': Float,
                'FEB_2022': Float,
                'MAR_2022': Float,
                'APR_2022': Float,
                'MAY_2022': Float,
                'JUN_2022': Float,
                'JUL_2022': Float,
                'AUG_2022': Float,
                'SEP_2022': Float,
                'OCT_2022': Float,
                'NOV_2022': Float,
                'DEC_2022': Float,
                'AVERAGE_2022': Float,
                'JAN_2023': Float,
                'FEB_2023': Float,
                'MAR_2023': Float,
                'APR_2023': Float,
                'MAY_2023': Float,
                'JUN_2023': Float,
                'JUL_2023': Float,
                'AUG_2023': Float,
                'SEP_2023': Float,
                'OCT_2023': Float,
                'NOV_2023': Float,
                'DEC_2023': Float,
                'AVERAGE_2023': Float,
                'AU_CREATED_DATE': DateTime(timezone=False),
                'AU_UPDATED_DATE': DateTime(timezone=False)
            }
        )
        logger.info("Successfully appended DataFrame to Redshift.")

    except Exception as e:
        logger.error(f"Error writing DataFrame to Redshift: {str(e)}")
        raise
    finally:
        engine.dispose()

# Function to move a file to the 'loaded' folder in S3
def move_file_to_loaded_folder(s3_client, s3_bucket, s3_file_path):
    try:
        loaded_folder_path = os.path.dirname(s3_file_path.replace("Raw_files", "loaded"))
        if not s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=loaded_folder_path):
            s3_client.put_object(Bucket=s3_bucket, Key=loaded_folder_path + '/')

        new_s3_file_path = loaded_folder_path + '/' + os.path.basename(s3_file_path)
        s3_client.copy_object(Bucket=s3_bucket, CopySource={'Bucket': s3_bucket, 'Key': s3_file_path}, Key=new_s3_file_path)
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_file_path)

        logger.info(f"Moved file to '{loaded_folder_path}' folder. New path: {new_s3_file_path}")
    except Exception as e:
        logger.error(f"Error moving the file to the '{loaded_folder_path}' folder: {str(e)}")
        raise

# Main execution of the script
def main():
    try:
        s3_client = boto3.client('s3')
        
        logger.info("Getting the latest Excel file from the Raw_files folder.")
        latest_excel_file = get_latest_excel_file(s3_client, s3_bucket, s3_path)
        
        if latest_excel_file is not None:
            logger.info(f"Latest Excel file: {latest_excel_file}")
            df_main = read_excel_from_s3(s3_client, s3_bucket, latest_excel_file)

            logger.info("Obtaining Redshift credentials.")
            db_credentials = obtain_credentials(boto3.client('secretsmanager', region_name='us-west-2'), secret_name)
            logger.info(f"db_creds: {db_credentials}")

            connection_string = f"postgresql+psycopg2://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{redshift_port}/{redshift_database}"
            engine = create_engine(connection_string)

            write_to_redshift(engine, df_main, table_name, redshift_schema)

            move_file_to_loaded_folder(s3_client, s3_bucket, latest_excel_file)
        else:
            logger.warning("No latest Excel file found. Exiting without processing.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
