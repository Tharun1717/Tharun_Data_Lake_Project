import redshift_connector
import boto3
import json
from datetime import datetime, timedelta
import logging
import pytz

# ====================================================
#              EMAIL NOTIFICATION
# ====================================================
TOPIC_ARN = 'arn:aws:sns:us-west-2:598936376082:JetParserNotifications'
REGION = 'us-west-2'

# ====================================================
#                   OBTAIN DATE
# ====================================================
eastern_tz = pytz.timezone("America/New_York")
yesterday = datetime.now(eastern_tz) - timedelta(days=1)
date = yesterday.strftime('%Y%m%d')
# date = '2023/10/28'


# ====================================================
#                   BOTO3 CLIENTS
# ====================================================
# SNS
sns = boto3.client('sns', region_name=REGION)
# Create secret client
secrets_client = boto3.client('secretsmanager', region_name='us-west-2')

# ====================================================
#                   PARAMETERS
# ====================================================

# Define secret name
secret_name = 'uat/Redshift/thg'

# Define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# db_name
db_name = 'lvi3plp-stg'

# interfaces
interfaces = ["DOCUMENT", "ADJUSTMENT", "APPOINTMENT", "TRADING_PARTNERS", "ITEMINFO", "LOCATION_MASTER", "SNAPSHOT",
              "MANUALKPI"]


def obtain_credentials(secrets_client, secret_name):
    try:
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
    except Exception as e:
        logger.exception("Exception occurred while obtaining credentials")
        raise e


def get_table_counts(db_credentials, db_name, date, interfaces):
    try:
        counts = {}

        with redshift_connector.connect(
                host=db_credentials['host'],
                port=5439,
                database=db_name,
                user=db_credentials['username'],
                password=db_credentials['password']

        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT filetype, sum(n_files) FROM ORION_EDW.JSON_FACILITY_FILE_COUNTS WHERE parsing_date = '{date}' group by 1")

                for row in cur.fetchall():
                    filetype, count = row
                    counts[filetype] = count

        check_interfaces = [interface for interface in interfaces if interface not in counts.keys()]

        return counts, check_interfaces
    except Exception as e:
        logger.exception("Exception occurred while getting table counts")
        raise e


def email_notification(sns, counts, check, check_interfaces, TOPIC_ARN):
    try:
        if check == 'SUCCESS':
            subject = f"JETPARSER STATUS - SUCESS - UAT"

            result_str = ""
            for key, value in sorted(counts.items()):
                result_str += f"- File Type {str(key)}: {str(value)} files parsed\n"

            text = f"""

The JetParser pipeline is working properly. Yesterday the process parsed the following amount of files:

{result_str}

            """

        elif check == 'CHECK':
            subject = f"JETPARSER STATUS - CHECK - UAT"

            check_str = ""
            for interface in sorted(check_interfaces):
                check_str += f"- {interface}\n"

            result_str = ""
            for key, value in sorted(counts.items()):
                result_str += f"- File Type {str(key)}: {str(value)} files parsed\n"

            text = f"""

The JetParser worked properly for the following interfaces:

{result_str}

Please check these interfaces since Redshift tables were not updated:

{check_str}

You should have receive an email from OrionXMLProcessor@americold.com explaining what happened.



            """


        elif check == 'FAIL':
            subject = f"JETPARSER STATUS - FAIL - UAT"
            text = f"""

IMPORTANT - We are not receiving files to parse to the bronze layer. Please check the process with Sudhanshu/Bhavna.

            """

        text += """
For any further information, please contact Hackett Tech Team.

Kind regards,
Hackett Tech Team
"""

        sns.publish(
            TopicArn=TOPIC_ARN,
            Message=text,
            Subject=subject,
        )

    except Exception as ex:
        logger.exception("Error in send_email_notification")
        raise ex


def main():
    try:
        logger.info("Obtaining credentials.")
        db_credentials = obtain_credentials(secrets_client, secret_name)

        logger.info("Getting table counts.")
        counts, check_interfaces = get_table_counts(db_credentials, db_name, date, interfaces)

        # Define statusa
        if len(check_interfaces) == 0:
            status = "SUCCESS"
        elif len(check_interfaces) > 0:
            status = "CHECK"
        else:
            status = "FAIL"

        if all(value >= 1 for value in
               counts.values()) and status == "SUCCESS":  # If there are any counts (i.e., the process is 'OK')
            logger.info("Sending OK email.")
            email_notification(sns, counts, status, check_interfaces, TOPIC_ARN)
        elif any(value >= 1 for value in counts.values()) and status == 'CHECK':
            email_notification(sns, counts, status, check_interfaces, TOPIC_ARN)
        else:  # If there are no counts (i.e., the process has 'FAIL'ed)
            logger.info("Sending FAIL email.")
            email_notification(sns, counts, 'FAIL', check_interfaces, TOPIC_ARN)
    except Exception as e:
        logger.exception("Exception occurred in main")
        raise e


if __name__ == "__main__":
    main()