import json
import os
import pymongo
import boto3

import urllib
from pymongo import MongoClient
from bson import ObjectId

DOC_DB_SECRET_NAME = 'thg/docdb/dev'
DOC_DB_NAME = 'americold-cloud-common'
SALESFORCE_PROGRAM_EVENT_COLLECTION_NAME = 'salesforce_programs'
mongo_cert_file_name_qa = 'global-bundle.cer'
print('dirname:     ', os.path.dirname(__file__))
rds_combined_ca_bundle_cer_path = os.path.dirname(__file__) + f"/{mongo_cert_file_name_qa}"



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

def createMongoClient():
    
    
    host = doc_db_credentials["host"]
    port = doc_db_credentials["port"]
    authdb = doc_db_credentials["authdb"]
    user = doc_db_credentials["user"]
    password = doc_db_credentials["password"]


    # Parse the connection string
    # parsed_url = urllib.parse.urlparse(mongo_conn_str)

    # # Extract username and password
    # user = parsed_url.username
    # password = parsed_url.password

    # # Extract host and port
    # host = parsed_url.hostname
    # port = parsed_url.port



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



        # Extract keys for the upsert operation
        facility_id = event_details['payload']['Facility_ID__c']
        wms_number = event_details['payload']['WMS_Number__c']
        agreement_program_id = event_details['payload']['Agreement_Program_ID__c']


        print(f"facility_id {facility_id}")
        print(f"wms_number {wms_number}")
        print(f"agreement_program_id {agreement_program_id}")


        # Create/Update document in DocumentDB
        db_client = createMongoClient()
        db = db_client[DOC_DB_NAME]
        collection = db[SALESFORCE_PROGRAM_EVENT_COLLECTION_NAME]

        # Define the filter for upsert operation
        filter_criteria = {
            'Agreement_Program_ID__c': agreement_program_id
        }

        # Perform upsert operation
        collection.update_one(filter_criteria, {"$set": event_details['payload']}, upsert=True)


        # Define the index
        indexes = []
        index_fields = [('Facility_ID__c', pymongo.ASCENDING), 
                        ('WMS_Number__c', pymongo.ASCENDING), 
                        ('Agreement_Program_ID__c', pymongo.ASCENDING)]
        
        index_list = [
            {
                'name': 'salesforce_program_feed_uniquekey_index_1', 
                'fields': [
                    ('Agreement_Program_ID__c', pymongo.ASCENDING)
                ],
                'unique': True
            }, 
            {
                'name': 'salesforce_program_feed_key_index_1',
                'fields': [
                    ('Facility_ID__c', pymongo.ASCENDING), 
                    ('WMS_Number__c', pymongo.ASCENDING), 
                    ('Agreement_Program_ID__c', pymongo.ASCENDING)
                ],
                'unique': False
            }
        ]

        # Check if the index already exists
        existing_indexes = collection.list_indexes()
        for index_obj in index_list:
            if not any(index['name'] == index_obj['name'] for index in existing_indexes):
                # Create the index if it doesn't exist
                collection.create_index(index_obj['fields'], name=index_obj['name'], unique=index_obj['unique'])

        closeMongoClient(db_client)
        
        print("successfully inserted/updated event")
        
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
