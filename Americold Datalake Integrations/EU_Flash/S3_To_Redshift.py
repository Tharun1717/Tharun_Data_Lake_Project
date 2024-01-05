import boto3
import psycopg2
import logging
import json
import redshift_connector
from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta

s3_bucket = 'i3pl-json-silver-dev'
folder = "EU_FLASH_CONSOLIDATED"

# Replace these values with your own
db_name = 'dev'
table_name = 'HACKETT_POC.ORION_EU_FLASH_CONSOLIDATED'
secret_name = 'dev/Redshift/thg'
iam_role = 'arn:aws:iam::646632670602:role/RedshiftRole'

# Define SES parameters
source_email = 'OrionXMLProcessor@americold.com'
to_emails = ['mohanreddy.kotha@thehackettgroup.com']

# Create a Redshift client
redshift = boto3.client('redshift')

# Create secret client
secrets_client = boto3.client('secretsmanager', region_name='us-west-2')

# Define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Establish a session with AWS
session = boto3.Session()

# Create clients for Redshift, SES, and S3
redshift_client = session.client('redshift')
ses_client = session.client('ses')
s3_client = session.client('s3')


# Define AWS Redshift and S3 parameters

def obtain_credentials(secrets_client, secret_name):
    # Get the Redshift cluster credentials from Secrets Manager
    get_secret_value_response = secrets_client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = json.loads(get_secret_value_response['SecretBinary'])

    db_credentials = json.loads(secret)

    return db_credentials


def send_failure_email(ses_client, source_email, to_emails, s3_path, error):
    """
    Send an email to specified recipients about a failure in the data loading process.

    Parameters:
    - ses_client: The boto3 SES client.
    - source_email: The email address from which to send the email.
    - to_emails: A list of email addresses to which to send the email.
    - s3_path: The S3 path for which the data loading failed.
    - error: The error that occurred.
    """
    try:
        # Create a formatted email body
        body = (
            "Dear user,\n\n"
            "The data load from '{}' to Redshift failed. Please find the error details below:\n\n"
            "{}\n\n"
            "Please check the files in the above S3 path.\n\n"
            "Best,\n"
            "Flash EU Process"
        ).format(s3_path, str(error))

        # Send the email
        ses_client.send_email(
            Source=source_email,
            Destination={
                'ToAddresses': to_emails
            },
            Message={
                'Subject': {
                    'Data': "Redshift data load failed"
                },
                'Body': {
                    'Text': {
                        'Data': body
                    }
                }
            }
        )

    except Exception as e:
        print(f"Error sending email: {e}")


def main():
    # Obtain credentials
    logger.info("1. Obtaining redshift credentials.")
    db_credentials = obtain_credentials(secrets_client, secret_name)
    logger.info(f"db_creds: {db_credentials}")

    # Connect to Redshift
    logger.info("2. Connecting to Redshift.")
    conn = redshift_connector.connect(
        host=db_credentials['host'],
        port=db_credentials['port'],
        database=db_name,
        user=db_credentials['username'],
        password=db_credentials['password'],
    )

    cursor = conn.cursor()

    # Prepare the date to load
    load_date = datetime.now().date()
    failed_files = []
    logger.info(f"3. Prepare the date to load {load_date}")

    s3_path = f"s3://{s3_bucket}/{folder}/{load_date.strftime('%Y/%m/%d')}/"
    logger.info(f"s3_path: {s3_path}")

    try:
        # Run the COPY command
        logger.info("Running COPY command")
        cursor.execute(f"""
        COPY {table_name} 
        FROM '{s3_path}' 
        IAM_ROLE '{iam_role}' 
        FORMAT JSON 'auto'
        GZIP
        ;
        """)
        # Commit COPY transaction
        conn.commit()

        print(f"Data for {load_date} loaded successfully")

    except Exception as ex:
        logger.error(f"Error loading data for {load_date}: {ex}")
        failed_files.append(s3_path)
        send_failure_email(ses_client, source_email, to_emails, s3_path, ex)
        conn.rollback()  # Rollback the transaction in case of failure

    if failed_files:
        logger.info(f"Data loading failed for the following dates: {', '.join(failed_files)}")

    conn.close()


if __name__ == '__main__':
    main()


