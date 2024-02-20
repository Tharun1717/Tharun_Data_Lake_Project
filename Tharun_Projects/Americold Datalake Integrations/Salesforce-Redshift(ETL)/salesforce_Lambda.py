import json
import os
import pymongo
import boto3
import urllib
import datetime
import time



DOC_DB_SECRET_NAME = 'thg/docdb/dev'
DOC_DB_NAME = 'americold-cloud-common'
SALESFORCE_PROGRAM_EVENT_COLLECTION_NAME = 'salesforce_programs'
mongo_cert_file_name_qa = 'global-bundle.cer'
print('dirname:     ', os.path.dirname(__file__))
rds_combined_ca_bundle_cer_path = os.path.dirname(__file__) + f"/{mongo_cert_file_name_qa}"



REDSHIFT_DB ="dev"
REDSHIFT_SCHEMA = "orion_edw"
REDSHIFT_SECRET_NAME = 'dev/Redshift/thg'
REDSHIFT_SECRET_ARN = "arn:aws:secretsmanager:us-west-2:646632670602:secret:dev/Redshift/thg-jkYezn"

SALESFORCE_PROGRAM_TABLE_NAME = 'program_id_data'



def get_credentials_from_secrets_manager(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager', region_name = 'us-west-2')
    
    # Retrieve the secret value
    response = client.get_secret_value(SecretId=secret_name)
    
    
    if 'SecretString' in response: 
        secret = response['SecretString'] 
    elif 'SecretBinary' in response:
        secret = json.loads(response['SecretBinary'])
    else:
        raise ValueError("Secrets Manager response format not recognized.")
    db_credentials = json.loads(secret)
    
    return db_credentials


doc_db_credentials = get_credentials_from_secrets_manager(DOC_DB_SECRET_NAME)
print(f'doc_db_credentials - fetched')

db_credentials = get_credentials_from_secrets_manager(REDSHIFT_SECRET_NAME)
print(f'redshift db_credentials - fetched')


def execute_redshift_query(query):

    redshift_secret_arn = REDSHIFT_SECRET_ARN
    database = REDSHIFT_DB
    
    
    # Create the Redshift Data API client
    client = boto3.client('redshift-data')

    # Execute the query using the Data API
    response = client.execute_statement(
        ClusterIdentifier=db_credentials['dbClusterIdentifier'],
        Database=database,
        SecretArn=redshift_secret_arn,
        Sql=query
    )

    print(f"Query response {response}")
    query_id = response['Id']

    # detail_response = client.describe_statement(Id=response['Id'])
    
    # print(f"Query detail_response {detail_response}")

    wait_for_query_execution(query_id, client)
    
    try:
        # Retrieve and return the query result
        result_response = client.get_statement_result(Id=query_id)
        # Format the query result in JSON format with column names as keys
        formatted_result = format_query_result(result_response)
    except Exception as e:
        print("client.get_statement_result not applicable since it could be a procedure call that returns no response")
        formatted_result = None
        
    return formatted_result

def wait_for_query_execution(query_id, client):
    while True:
        response = client.describe_statement(Id=query_id)
        status = response['Status']
        if status == 'FINISHED':
            break
        elif status in ('FAILED', 'CANCELLED'):
            raise ValueError(f"Query execution failed or was cancelled. Status: {status}")
        time.sleep(1)

def format_query_result(result_response):
    # Extract column metadata
    column_metadata = result_response['ColumnMetadata']
    column_names = [column['name'] for column in column_metadata]

    # Extract the rows from the result set
    result_rows = result_response['Records']

    # Format the result as a list of dictionaries
    formatted_result = []
    for row in result_rows:
        formatted_row = {}
        for i, column_value in enumerate(row):
            formatted_row[column_names[i]] = column_value.get('stringValue', None)
        formatted_result.append(formatted_row)

    return formatted_result

def save_to_redshift(program):
        
    current_datetime = datetime.datetime.now()
    current_timestamp_without_timezone = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    # Function to convert None to NULL
    def convert_to_null(value):
        if value is None:
            return 'NULL'
        else:
            return f"'{value}'"


    def convert_to_stringified_json(value):
        if value is None:
            json_value = []
        else:
            json_value = value

        json_value_str = json.dumps(json_value)
        json_value_str_escaped = json_value_str.replace("'", "''")
        return f"'{json_value_str_escaped}'"

    agreement_program_id__c = program.get("agreement_program_id__c")

    upsert_program_query = f"""
        UPDATE {REDSHIFT_SCHEMA}.{SALESFORCE_PROGRAM_TABLE_NAME}
        SET
            annual_committed_amount__c = {convert_to_null(program.get("annual_committed_amount__c"))},
            annual_committed_revenue__c = {convert_to_null(program.get("annual_committed_revenue__c"))},
            au_updated_date = '{current_timestamp_without_timezone}',
            commitment__c = {convert_to_null(program.get("commitment__c"))},
            commitment_calendar_type__c = {convert_to_null(program.get("commitment_calendar_type__c"))},
            commitment_type__c = {convert_to_null(program.get("commitment_type__c"))},
            committed_unit_of_measure__c = {convert_to_null(program.get("committed_unit_of_measure__c"))},
            createdbyid = {convert_to_null(program.get("createdbyid"))},
            createddate = {convert_to_null(program.get("createddate"))},
            currencyisocode__c = {convert_to_null(program.get("currencyisocode__c"))},
            customer_blanket_po__c = {convert_to_null(program.get("customer_blanket_po__c"))},
            distribution_channel__c = {convert_to_null(program.get("distribution_channel__c"))},
            facility_id__c = {convert_to_null(program.get("facility_id__c"))},
            inbound_time_based_charge__c = {convert_to_null(str(program.get("inbound_time_based_charge__c")).upper())},
            initial_wra_id__c = {convert_to_null(program.get("initial_wra_id__c"))},
            item_family_payload__c = {convert_to_stringified_json(program.get("item_family_payload__c"))},
            layer_pick__c = {convert_to_null(str(program.get("layer_pick__c")).upper())},
            oracle_business_unit__c = {convert_to_null(program.get("oracle_business_unit__c"))},
            partial_billing_integration__c = {convert_to_null(str(program.get("partial_billing_integration__c")).upper())},
            payment_terms__c = {convert_to_null(program.get("payment_terms__c"))},
            program_status__c = {convert_to_null(program.get("program_status__c"))},
            rates_payload__c = {convert_to_stringified_json(program.get("rates_payload__c"))},
            remit_city__c = {convert_to_null(program.get("remit_city__c"))},
            remit_country__c = {convert_to_null(program.get("remit_country__c"))},
            remit_postal_code__c = {convert_to_null(program.get("remit_postal_code__c"))},
            remit_state__c = {convert_to_null(program.get("remit_state__c"))},
            remit_street__c = {convert_to_null(program.get("remit_street__c"))},
            retail_billing__c = {convert_to_null(str(program.get("retail_billing__c")).upper())},
            sfdc_billing_site_id__c = {convert_to_null(program.get("sfdc_billing_site_id__c"))},
            sfdc_customer_number__c = {convert_to_null(program.get("sfdc_customer_number__c"))},
            shared_commitment_id__c = {convert_to_null(program.get("shared_commitment_id__c"))},
            site_center__c = {convert_to_null(program.get("site_center__c"))},
            supplier_billing__c = {convert_to_null(str(program.get("supplier_billing__c")).upper())},
            temperature_type__c = {convert_to_null(program.get("temperature_type__c"))},
            weight_billing_type__c = {convert_to_null(program.get("weight_billing_type__c"))},
            wms_number__c = {convert_to_null(program.get("wms_number__c"))}
        WHERE agreement_program_id__c = '{agreement_program_id__c}';
    
        INSERT INTO {REDSHIFT_SCHEMA}.{SALESFORCE_PROGRAM_TABLE_NAME}
            (
                agreement_program_id__c,
                annual_committed_amount__c, 
                annual_committed_revenue__c,
                au_created_date,
                au_updated_date,
                commitment__c,
                commitment_calendar_type__c,
                commitment_type__c,
                committed_unit_of_measure__c,
                createdbyid,
                createddate,
                currencyisocode__c,
                customer_blanket_po__c,
                distribution_channel__c,
                facility_id__c,
                inbound_time_based_charge__c,
                initial_wra_id__c,
                item_family_payload__c,
                layer_pick__c,
                oracle_business_unit__c,
                partial_billing_integration__c,
                payment_terms__c,
                program_status__c,
                rates_payload__c,
                remit_city__c,
                remit_country__c,
                remit_postal_code__c,
                remit_state__c,
                remit_street__c,
                retail_billing__c,
                sfdc_billing_site_id__c,
                sfdc_customer_number__c,
                shared_commitment_id__c,
                site_center__c,
                supplier_billing__c,
                temperature_type__c,
                weight_billing_type__c,
                wms_number__c
            )
        SELECT 
            '{agreement_program_id__c}',
            {convert_to_null(program.get("annual_committed_amount__c"))}, 
            {convert_to_null(program.get("annual_committed_revenue__c"))},
            '{current_timestamp_without_timezone}',
            '{current_timestamp_without_timezone}',
            {convert_to_null(program.get("commitment__c"))},
            {convert_to_null(program.get("commitment_calendar_type__c"))},
            {convert_to_null(program.get("commitment_type__c"))},
            {convert_to_null(program.get("committed_unit_of_measure__c"))},
            {convert_to_null(program.get("createdbyid"))},
            {convert_to_null(program.get("createddate"))},
            {convert_to_null(program.get("currencyisocode__c"))},
            {convert_to_null(program.get("customer_blanket_po__c"))},
            {convert_to_null(program.get("distribution_channel__c"))},
            {convert_to_null(program.get("facility_id__c"))},
            {convert_to_null(str(program.get("inbound_time_based_charge__c")).upper())},
            {convert_to_null(program.get("initial_wra_id__c"))},
            {convert_to_stringified_json(program.get("item_family_payload__c"))},
            {convert_to_null(str(program.get("layer_pick__c")).upper())},
            {convert_to_null(program.get("oracle_business_unit__c"))},
            {convert_to_null(str(program.get("partial_billing_integration__c")).upper())},
            {convert_to_null(program.get("payment_terms__c"))},
            {convert_to_null(program.get("program_status__c"))},
            {convert_to_stringified_json(program.get("rates_payload__c"))},
            {convert_to_null(program.get("remit_city__c"))},
            {convert_to_null(program.get("remit_country__c"))},
            {convert_to_null(program.get("remit_postal_code__c"))},
            {convert_to_null(program.get("remit_state__c"))},
            {convert_to_null(program.get("remit_street__c"))},
            {convert_to_null(str(program.get("retail_billing__c")).upper())},
            {convert_to_null(program.get("sfdc_billing_site_id__c"))},
            {convert_to_null(program.get("sfdc_customer_number__c"))},
            {convert_to_null(program.get("shared_commitment_id__c"))},
            {convert_to_null(program.get("site_center__c"))},
            {convert_to_null(str(program.get("supplier_billing__c")).upper())},
            {convert_to_null(program.get("temperature_type__c"))},
            {convert_to_null(program.get("weight_billing_type__c"))},
            {convert_to_null(program.get("wms_number__c"))}
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {REDSHIFT_SCHEMA}.{SALESFORCE_PROGRAM_TABLE_NAME} 
            WHERE agreement_program_id__c = '{agreement_program_id__c}'
        );
    """
    print(upsert_program_query)
    execute_redshift_query(upsert_program_query)




def createMongoClient():
    
    
    host = doc_db_credentials["host"]
    port = doc_db_credentials["port"]
    authdb = doc_db_credentials["authdb"]
    user = doc_db_credentials["user"]
    password = doc_db_credentials["password"]

    string_to_encode = password
    # URL encode the string
    password = urllib.parse.quote(string_to_encode)

    conn_str = f"mongodb://{user}:{password}@{host}:{port}/{authdb}?ssl=true&ssl_ca_certs=global-bundle.cer&tls=true&tlsCAFile={rds_combined_ca_bundle_cer_path}&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    
    # conn_str = mongo_conn_str
    # conn_str = f"{mongo_conn_str}&tls=true&tlsCAFile={rds_combined_ca_bundle_cer_path}" 
    
    print(f"conn_str - {conn_str}")
    
    return pymongo.MongoClient(conn_str) 


def closeMongoClient(client):
    client.close()


def lowercase_keys(obj):
    if isinstance(obj, dict):
        return {k.lower(): lowercase_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [lowercase_keys(element) for element in obj]
    else:
        return obj

# Process each record from the SQS event
def process_record(record):
    
    message_body = None
    db_client = None
    try:
        print(f"Raw record body: {record['body']}")
        
        # Load the message
        message_body = json.loads(record['body'])
        print(f"Parsed message body: {message_body}")

        event_details = message_body['detail']
        print(f"event_details {event_details}")


        if 'payload' in event_details:
            payload = event_details['payload']
            
            # Check and parse 'Rates_Payload__c'
            if 'Rates_Payload__c' in payload and payload['Rates_Payload__c']:
                payload['Rates_Payload__c'] = json.loads(payload['Rates_Payload__c'])
            
            # Check and parse 'Item_Family_Payload__c'
            if 'Item_Family_Payload__c' in payload and payload['Item_Family_Payload__c']:
                payload['Item_Family_Payload__c'] = json.loads(payload['Item_Family_Payload__c'])



        event_details = lowercase_keys(event_details)
        print(event_details)


        # Extract keys for the upsert operation
        facility_id = event_details['payload']['facility_id__c']
        wms_number = event_details['payload']['wms_number__c']
        agreement_program_id = event_details['payload']['agreement_program_id__c']


        print(f"facility_id {facility_id}")
        print(f"wms_number {wms_number}")
        print(f"agreement_program_id {agreement_program_id}")


        # Create/Update document in DocumentDB
        db_client = createMongoClient()
        db = db_client[DOC_DB_NAME]
        collection = db[SALESFORCE_PROGRAM_EVENT_COLLECTION_NAME]

        # Define the filter for upsert operation
        filter_criteria = {
            'agreement_program_id__c': agreement_program_id
        }

        # Perform upsert operation
        collection.update_one(filter_criteria, {"$set": event_details['payload']}, upsert=True)


        # Define the index
        
        index_list = [
            {
                'name': 'salesforce_program_feed_uniquekey_index_1', 
                'fields': [
                    ('agreement_program_id__c', pymongo.ASCENDING)
                ],
                'unique': True
            }, 
            {
                'name': 'salesforce_program_feed_key_index_1',
                'fields': [
                    ('facility_id__c', pymongo.ASCENDING), 
                    ('wms_number__c', pymongo.ASCENDING), 
                    ('agreement_program_id__c', pymongo.ASCENDING)
                ],
                'unique': False
            }
        ]

        # Check if the index already exists
        existing_indexes = collection.list_indexes()
        print(f"existing_indexes {existing_indexes}")
        print(f"index_list to be ensured {index_list}")
        for index_obj in index_list:
            if not any(index['name'] == index_obj['name'] for index in existing_indexes):
                # Create the index if it doesn't exist
                collection.create_index(index_obj['fields'], name=index_obj['name'], unique=index_obj['unique'])

        closeMongoClient(db_client)
        
        print("successfully inserted/updated event into docdb")
        
        
        save_to_redshift(event_details['payload'])
        print("successfully inserted/updated event into redshift")

        
    except Exception as e:
        print(f"Error processing record: {e} message_body: {message_body if message_body else 'message_body not available'}")
        
        if db_client:
            closeMongoClient(db_client)

# Main Lambda handler
def lambda_handler(event, context):
    for record in event['Records']:
        process_record(record)

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
