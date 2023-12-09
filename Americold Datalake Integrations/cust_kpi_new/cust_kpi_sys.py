import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import boto3
from datetime import datetime, timedelta
import pip._internal

pip._internal.main(['install', 'redshift_connector'])
import redshift_connector
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

run_time = datetime.now().strftime("%Y%m%d%H%M%S")

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

# Get facilities
query_string = f"""SELECT DISTINCT source_system_code, facility_id, report_date
                FROM "lvi3plp-stg"."orion_edw"."inventory_consolidated"
                WHERE report_date='{yesterday_str}';"""

cursor = conn.cursor()
logging.info(f"Executing query: {query_string}")
cursor.execute(query_string)
tup_facilities = cursor.fetchall()
logging.info(tup_facilities)
conn.commit()
conn.autocommit = True

all_facilities = set()
for facility_info in tup_facilities:
    source_system, facility, report_date = facility_info
    all_facilities.add((source_system, facility, report_date))

glue_client = boto3.client("glue")
response = glue_client.get_job_runs(JobName='orion_cust_kpi_sys_staging_daily')
glue_job_uid = response["JobRuns"][0]["Id"]


def sp_cust_kpi_processor(source_system, facility, run_time, no_of_days, cursor):
    end_date = run_time + timedelta(days=no_of_days)

    occupancy = f"call {REDSHIFT_SCHEMA}.proc_custkpi_processor_occupancy('{source_system}', '{facility}', '{run_time}', '{end_date}','{glue_job_uid}')"
    logging.info(occupancy)
    cursor.execute(occupancy)

    conn.commit()
    conn.autocommit = True
    job.commit()

    for i in range(no_of_days):
        execution_date = run_time + timedelta(days=i)
        execution_date_str = execution_date.strftime('%Y-%m-%d')

        # # All Metrics
        allMetrics = f"call {REDSHIFT_SCHEMA}.proc_custkpi_sys('{source_system}', '{facility}', '{execution_date_str}','{glue_job_uid}')"
        logging.info(allMetrics)
        cursor.execute(allMetrics)

        conn.commit()
        conn.autocommit = True
        job.commit()


def sp_cust_kpi_calculations(source_system, facility, run_time, no_of_days, cursor):
    end_date = run_time + timedelta(days=no_of_days)

    # # All KPIs
    calc_allKPIs = f"call {REDSHIFT_SCHEMA}.proc_custkpi_sys_merge('{facility}', '{source_system}', '{run_time}', '{end_date}','{glue_job_uid}')"
    logging.info(calc_allKPIs)
    # cursor.execute(calc_allKPIs)
    conn.cursor().execute(calc_allKPIs)

    conn.commit()
    conn.autocommit = True
    job.commit()


for facility_info in all_facilities:
    source_system, facility, report_date = facility_info
    source_system = source_system.strip()
    facility = facility.strip()

    print("facility:", facility)
    print("source_system:", source_system)

    sp_cust_kpi_processor(source_system, facility, report_date, 1, cursor)
    sp_cust_kpi_calculations(source_system, facility, report_date, 1, cursor)

conn.autocommit = True
cursor.close()
conn.close()