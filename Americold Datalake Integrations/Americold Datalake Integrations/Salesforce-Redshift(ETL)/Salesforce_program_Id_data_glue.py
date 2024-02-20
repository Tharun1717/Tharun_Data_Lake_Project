import boto3
import logging
import json
import redshift_connector
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta
import concurrent.futures

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# Setting timezone
yesterday = datetime.now() - timedelta(days=1)
date = yesterday.strftime("%Y/%m/%d")

# Redshift Configuration
redshift_port = 5439
redshift_database = 'dev'
redshift_schema = 'orion_edw'
table_name = 'program_id_data'
secret_name = 'dev/Redshift/thg'
iam_role = 'arn:aws:iam::646632670602:role/RedshiftRole'

# S3 Configuration
source_bucket = 'i3pl-json-silver-dev'
source_prefix = f'SALESFORCE_DATA/Raw_Files/{date}/'
destination_bucket = 'i3pl-json-silver-dev'
destination_prefix = f'SALESFORCE_DATA/Loaded_Files/{date}/'

# AWS Region
aws_region = 'us-west-2'

# Define SES parameters
source_email = 'OrionXMLProcessor@americold.com'
to_emails = ['mohanreddy.kotha@thehackettgroup.com']

# Establish a session with AWS
session = boto3.Session()

# Create clients for Redshift, SES, and S3
redshift_client = session.client('redshift')
ses_client = session.client('ses')
s3_client = session.client('s3')

class NoFilesFoundError(Exception):
    pass
def move_files(bucket_name, source_prefix, destination_prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = [obj for obj in bucket.objects.filter(Prefix=source_prefix) if isValidFile(obj.key, source_prefix, s3)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(move_file, s3, bucket_name, obj, destination_prefix) for obj in objects]
        concurrent.futures.wait(futures)

    # Check if all files have been moved successfully
    if all([(obj.key.replace(source_prefix, destination_prefix, 1) == obj.key) for obj in objects]):
        # Delete the source folder
        bucket.objects.filter(Prefix=source_prefix).delete()

    return len(objects)

def move_file(s3, bucket_name, obj, destination_prefix):
    new_key = obj.key.replace(source_prefix, destination_prefix, 1)
    # Copy object to the new destination
    s3.Object(bucket_name, new_key).copy_from(CopySource={'Bucket': bucket_name, 'Key': obj.key})
    # Delete the original object
    s3.Object(bucket_name, obj.key).delete()
    logger.info(f"Moved {obj.key} to {new_key}")

def isValidFile(file_key, source_prefix, s3):
    try:
        # Download the JSON file from S3 and attempt to parse it as JSON
        file_content = s3.Object(source_bucket, file_key).get()['Body'].read().decode('utf-8')
        json.loads(file_content)
        return True
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Invalid JSON file: {file_key}. Error: {str(e)}")
        return False

def send_email(subject, body):
    try:
        ses_client.send_email(
            Source=source_email,
            Destination={
                'ToAddresses': to_emails,
            },
            Message={
                'Subject': {
                    'Data': subject,
                },
                'Body': {
                    'Text': {
                        'Data': body,
                    },
                },
            },
        )
        logger.info(f"Email notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send email notification. Error: {str(e)}")

def obtain_credentials(secrets_client, secret_name):
    try:
        # Get the Redshift cluster credentials from Secrets Manager
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)

        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary'].decode('utf-8')

        db_credentials = json.loads(secret)
        return db_credentials
    except Exception as e:
        logger.error(f"Error obtaining Redshift credentials: {str(e)}")
        raise


def main():
    try:
        s3_client = boto3.client('s3', region_name=aws_region)

        logger.info("Obtaining Redshift credentials.")
        db_credentials = obtain_credentials(boto3.client('secretsmanager', region_name=aws_region), secret_name)
        logger.info(f"Redshift credentials obtained: {db_credentials}")

        # Connect to Redshift
        with redshift_connector.connect(
            host=db_credentials['host'],
            port=redshift_port,
            database=redshift_database,
            user=db_credentials['username'],
            password=db_credentials['password'],
        ) as conn:
            cursor = conn.cursor()

            # Running Redshift COPY command
            cursor.execute(f"""
                COPY "{redshift_schema}"."{table_name}"
                FROM 's3://{source_bucket}/{source_prefix}'
                IAM_ROLE '{iam_role}'
                FORMAT AS JSON 's3://{source_bucket}/SALESFORCE_DATA/JSON_PATH_FORMAT/JSON_PATH.json'
                TIMEFORMAT 'auto';
                """)

            # Set au_created_date and au_updated_date with the current timestamp
            cursor.execute(f"""
                UPDATE "{redshift_schema}"."{table_name}"
                SET au_created_date = CURRENT_TIMESTAMP,
                    au_updated_date = CURRENT_TIMESTAMP
                WHERE au_created_date IS NULL OR au_updated_date IS NULL;
                """)

            # Check if any files were loaded
            if cursor.rowcount == 0:
                raise NoFilesFoundError("No files found in the selected S3 path.")

            conn.commit()
            logger.info("Successfully ran Redshift COPY command and updated timestamps.")

            # Count the number of records processed
            records_processed = cursor.rowcount

            # Move files from source to destination
            moved_file_count = move_files(source_bucket, source_prefix, destination_prefix)
            logger.info(f"Total files moved: {moved_file_count}")

            # Count the number of records loaded on the specified date
            cursor.execute(f"""
                SELECT COUNT(*) AS records_loaded_on_date
                FROM "{redshift_schema}"."{table_name}"
                WHERE au_created_date::date = '{yesterday}';
                """)
            records_loaded_on_date = cursor.fetchone()[0]

            # Send email notification with records processed information
            subject = f"Salesforce_Events_Program_Id data Redshift Table Update - {date}"
            body = f"The Redshift table {table_name} has been updated with {records_processed} records for {date}."
            #body += f"\nNumber of records loaded on {date}: {records_loaded_on_date}"
            body += f"\nTotal files moved: {moved_file_count}"
            send_email(subject, body)

    except NoCredentialsError:
        logger.error("AWS credentials not available.")
        send_email("Error in Salesforce_Events_Program_Id Job", "AWS credentials not available.")
    except NoFilesFoundError as e:
        logger.warning(f"{str(e)} The script ran successfully, but no files were found.")
        send_email("No Files Found in Salesforce_Events_Program_Id", str(e))
    except Exception as e:
        logger.error(f"An unexpected error occurred in Salesforce_Events_Program_Id processing: {str(e)}")
        send_email("Error in Salesforce_Events_Program_Id Update", f"An unexpected error occurred: {str(e)}")
        raise
# Run the script
if __name__ == "__main__":
    main()
