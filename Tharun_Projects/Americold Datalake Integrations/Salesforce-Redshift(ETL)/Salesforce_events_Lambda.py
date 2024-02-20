import json
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
bucket_name = 'i3pl-json-silver-dev'
folder_name = 'SALESFORCE_DATA/Raw_Files'

def lambda_handler(event, context):
    try:
        # Log the entire data received from the event bus
        logger.info('Event Received: %s', json.dumps(event))

        detail = event.get('detail', {})
        payload = detail.get('payload', {})
        FacilityID = payload.get('Facility_ID__c', None)

        timestamp = datetime.utcnow()
        formatted_timestamp = timestamp.strftime('%Y%m%d%H%M%S')
        year, month, day = timestamp.strftime('%Y %m %d').split()

        event_id = event.get('id', 'unknown_event_id')

        if FacilityID is None:
            logger.warning(f'[WARNING]\t{timestamp}\t{event_id}\tFacilityID not found in payload')
            FacilityID = 'unknown_FacilityID'
        else:
            logger.info(f'[INFO]\t{timestamp}\t{event_id}\tFacilityID found in payload: {FacilityID}')

        data = json.dumps(payload)

        # Constructing the S3 key with FacilityID and current timestamp
        s3_key = f'{folder_name}/{year}/{month}/{day}/{FacilityID}/{FacilityID}_{formatted_timestamp}.json'

        s3.put_object(Body=data.encode('utf-8'), Bucket=bucket_name, Key=s3_key)

        return {
            'statusCode': 200,
            'body': json.dumps('Event processed and data stored in S3')
        }

    except Exception as e:
        timestamp = datetime.utcnow()
        event_id = event.get('id', 'unknown_event_id')
        logger.error(f'[ERROR]\t{timestamp}\t{event_id}\tError processing event: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing event')
        }
