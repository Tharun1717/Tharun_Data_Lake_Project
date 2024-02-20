import boto3
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Connect to SQS
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-west-2.amazonaws.com/646632670602/salesforce_queue'

def lambda_handler(event, context):
    try:
        # Log the entire data received from the event bus
        logger.info('Event Received: %s', json.dumps(event))

        event_details = event.get('detail', {})
        
        # Extract information from the payload
        payload = event_details.get('payload', {})
        sfdc_customer_number = payload.get('SFDC_Customer_Number__c', 'N/A')
        agreement_program_id = payload.get('Agreement_Program_ID__c', 'N/A')

        timestamp = datetime.utcnow()

        # Log extracted information
        logger.info('[INFO]\t%s\tSFDC Customer Number: %s', timestamp, sfdc_customer_number)
        logger.info('[INFO]\t%s\tAgreement Program ID: %s', timestamp, agreement_program_id)

        # Create a message to send to the SQS queue
        message_body = {
            'sfdc_customer_number': sfdc_customer_number,
            'agreement_program_id': agreement_program_id
            # Add more fields as needed
        }

        # Send the message to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )

        logger.info(f"Message sent to SQS: {response['MessageId']}")

        return {
            'statusCode': 200,
            'body': json.dumps('Event processed and data sent to SQS')
        }

    except Exception as e:
        timestamp = datetime.utcnow()
        logger.error(f'[ERROR]\t{timestamp}\tError processing event: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing event')
        }
