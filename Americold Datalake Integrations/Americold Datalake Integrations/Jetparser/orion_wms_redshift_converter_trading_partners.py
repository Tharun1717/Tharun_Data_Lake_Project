import boto3
import logging
import json
import redshift_connector
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import pytz
import os
import concurrent

s3_bucket = 'i3pl-json-silver-uat'
folder = "TRADING_PARTNERS"

# Replace these values with your own
db_name = 'lvi3plp-stg'
table_name = 'ORION_EDW.JSON_TRADING_PARTNERS_STG'  # ACA HAY QUE CAMBIAR A LA TABLA QUE VAYAMOS A USAR!!
table_load_name = 'ORION_EDW.JSON_JETPARSER_LOAD_STATUS_STG_TABLES'
secret_name = 'uat/Redshift/thg'
iam_role = 'arn:aws:iam::598936376082:role/Redshift_Role'

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

# Define SES parameters
source_email = 'OrionXMLProcessor@americold.com'
to_emails = ['mohanreddy.kotha@thehackettgroup.com']

# setting timezone
eastern_tz = pytz.timezone('America/New_York')


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


def get_last_loaded_date(cursor, table_load_name):
    cursor.execute(f"SELECT MAX(load_date) FROM {table_load_name} WHERE table_name = '{folder}'")
    result = cursor.fetchone()
    return result[0]


def insert_load_status(cursor, date, conn, table_load_name, dt_start, dt_end, qtyFilesProc):
    try:
        formatted_date = date.strftime('%Y-%m-%d')
        cursor.execute(f"""INSERT INTO {table_load_name}(LOAD_DATE,TABLE_NAME,EXEC_START,EXEC_END,FILES_PROCESSED) 
                        VALUES ('{formatted_date}', '{folder}',cast('{dt_start}' as timestamp),cast('{dt_end}' as timestamp),{qtyFilesProc})""")
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting load status: {e}")


def send_failure_email(ses_client, source_email, to_emails, s3_path, error, folder=folder):
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

            "Run the following query in Redshift to check which files failed:\n"
            "SELECT *\n"
            "FROM stl_load_errors\n"
            "ORDER BY starttime DESC\n"
            "LIMIT 10;\n\n"

            "Best,\n"
            "JetStream Parser Process"
        ).format(s3_path, str(error))

        # Send the email
        ses_client.send_email(
            Source=source_email,
            Destination={
                'ToAddresses': to_emails
            },
            Message={
                'Subject': {
                    'Data': f"Redshift data load failed - {folder}"
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


def createKeys(s3_client, s3_bucket, file_type, date):
    for folder_name in ["in_progress", "loaded"]:
        folder_path = f"{file_type}/{date.strftime('%Y/%m/%d')}/{folder_name}/"
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=f"{folder_path}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # key doesn't exists and needs to be created
                s3_client.put_object(Bucket=s3_bucket, Key=f"{folder_path}")
            else:
                raise


def isValidFile(sObjKey, sSourcePath):
    if (sObjKey.find('.json.gz') >= 0):  # Chek if it's a file and as well, if it's in the right DAY path
        sTmp = sObjKey.replace(sSourcePath, '')
        if (sTmp.find('/') >= 0):
            bResult = False
        else:
            bResult = True
    else:
        bResult = False
    return bResult


def move_file(s3, bucket_name, obj, source_folder, destination_folder):
    new_key = obj.key.replace(source_folder, destination_folder, 1)
    # Copy object to new destination
    s3.Object(bucket_name, new_key).copy_from(CopySource={'Bucket': bucket_name, 'Key': obj.key})
    # Delete the original object
    s3.Object(bucket_name, obj.key).delete()
    print(f"Moved {obj.key} to {new_key}")


def move_files(bucket_name, source_folder, destination_folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = [obj for obj in bucket.objects.filter(Prefix=source_folder) if
               (obj.key != source_folder and isValidFile(obj.key, source_folder))]

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(move_file, s3, bucket_name, obj, source_folder, destination_folder) for obj in
                   objects]
        concurrent.futures.wait(futures)
    return len(objects)


def checkPendingFilesToProcess(bucket_name, source_folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = [obj for obj in bucket.objects.filter(Prefix=source_folder) if
               (obj.key != source_folder and isValidFile(obj.key, source_folder))]
    return len(objects)


def main2():
    file_type = "TRADING_PARTNERS"
    date_to_process = datetime(2023, 10, 29).date()  # Change this to the desired date
    source_folder_key = f"{file_type}/{date_to_process.strftime('%Y/%m/%d')}/loaded/"
    destination_folder = f"{file_type}/{date_to_process.strftime('%Y/%m/%d')}/"
    move_files(s3_bucket, source_folder_key, destination_folder)


def main():
    try:
        ##################################################################################
        dt = datetime.now().date()  # Change this to the desired date
        # dt = datetime(2023, 10, 30).date()
        datesToCheck = [dt - timedelta(days=1),
                        dt]  # This is needed to check if there are pending files from previous date that needs to be loaded
        ##################################################################################

        file_type = "TRADING_PARTNERS"
        in_progress_folder_name = "in_progress"
        final_folder_name = "loaded"

        for date_to_process in datesToCheck:

            source_folder_key = f"{file_type}/{date_to_process.strftime('%Y/%m/%d')}/"
            in_progress_key = f"{source_folder_key}{in_progress_folder_name}/"
            final_folder_key = f"{source_folder_key}{final_folder_name}/"

            # Check if we have files to process for the date that I'm iterating
            iPendingFilesToProcess = checkPendingFilesToProcess(s3_bucket, source_folder_key)

            # If we have files, proceed and process them, if not, check the next date or exit
            if iPendingFilesToProcess > 0:
                logger.info(
                    f"0. {str(iPendingFilesToProcess)} files to process for date {date_to_process.strftime('%Y/%m/%d')}. Start process.")

                # Obtain credentials
                logger.info("1. Obtaining Redshift credentials.")
                db_credentials = obtain_credentials(secrets_client, secret_name)
                logger.info(f"db_creds: {db_credentials}")

                # Connect to Redshift
                logger.info("2. Connecting to Redshift.")
                conn = redshift_connector.connect(
                    host=db_credentials['host'],
                    port=5439,
                    database=db_name,
                    user=db_credentials['username'],
                    password=db_credentials['password'])

                source_folder_key = f"{file_type}/{date_to_process.strftime('%Y/%m/%d')}/"
                in_progress_folder_name = "in_progress"
                final_folder_name = "loaded"
                in_progress_key = f"{source_folder_key}{in_progress_folder_name}/"
                final_folder_key = f"{source_folder_key}{final_folder_name}/"

                # Create folders if they don't exist
                logger.info("3. Creating folder if they don't exist")
                createKeys(s3_client, s3_bucket, file_type, date_to_process)

                ### select and grab all files and move from / to in_progress
                logger.info("4. Moving Files to IN_PROGRESS folder")
                qtyFilesProc = move_files(s3_bucket, source_folder_key, in_progress_key)

                #### Load into redshift
                load_date = date_to_process
                failed_files = []
                dt_start = datetime.now().strftime("%Y%m%d %H:%M:%S")
                s3_path = f"s3://{s3_bucket}/{file_type}/{load_date.strftime('%Y/%m/%d')}/in_progress/"

                # Run the COPY command
                logger.info("5. Loading files into Redshift Table")
                cursor = conn.cursor()
                cursor.execute(f"""
                        COPY {table_name}(facility_id,source_system_code,filename,filename_chunk,parsing_date,xml_data) 
                        FROM '{s3_path}' 
                        IAM_ROLE '{iam_role}' 
                        FORMAT JSON 'auto'
                        GZIP;
                        """)
                # Commit COPY transaction
                conn.commit()

                dt_end = datetime.now().strftime("%Y%m%d %H:%M:%S")

                # Insert the date into the load_status table
                logger.info("6. Logging information on REDSHIFT table")
                insert_load_status(cursor, load_date, conn, table_load_name, dt_start, dt_end, qtyFilesProc)

                ### move all files from in_progress to loaded
                logger.info("7. Moving files from IN_PROGRESS to LOADED folder")
                qtyFilesProc = move_files(s3_bucket, in_progress_key, final_folder_key)

                logger.info(
                    f"8. {str(iPendingFilesToProcess)} files processed for date {date_to_process.strftime('%Y/%m/%d')}. End process.")

                if failed_files:
                    logger.info(
                        f"Data loading failed for {file_type} on the following dates: {', '.join(failed_files)}")

                conn.close()

    except Exception as ex:
        raise ex


if __name__ == '__main__':
    main()