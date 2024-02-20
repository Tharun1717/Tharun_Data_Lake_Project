import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import boto3
from datetime import datetime, timedelta
import pip

pip.main(['install', 'redshift_connector'])
import redshift_connector
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

run_time = datetime.now().strftime("%Y%m%d%h%m%s")

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

secrets_client = boto3.client('secretsmanager', region_name='us-west-2')
secret_name = 'uat/Redshift/thg'
get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)

if 'SecretString' in get_secret_value_response:
    secret = get_secret_value_response['SecretString']
else:
    secret = json.loads(get_secret_value_response['SecretBinary'])

db_credentials = json.loads(secret)
REDSHIFT_SCHEMA = "orion_edw"
REDSHIFT_DB = "lvi3plp-stg"

conn = redshift_connector.connect(
    host=db_credentials['host'],
    port=5439,
    database=REDSHIFT_DB,
    user=db_credentials['username'],
    password=db_credentials['password']
)

logging.info(f"Connection object: {conn}")

# Calculate yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

glue_client = boto3.client("glue")
response = glue_client.get_job_runs(JobName='orion_cust_kpi_sys_staging_daily')
glue_job_uid = response["JobRuns"][0]["Id"]

query_string = f"""
      CALL "{REDSHIFT_DB}"."{REDSHIFT_SCHEMA}".proc_custkpi_man('{yesterday_str}', '{glue_job_uid}')
      """

cursor = conn.cursor()
logging.info(f"Executing query: {query_string}")
cursor.execute(query_string)

# Commit the transaction
conn.commit()
conn.autocommit = True

# Commit the job
job.commit()

# Close the cursor and connection
cursor.close()
conn.close()

# Comment Added for testing