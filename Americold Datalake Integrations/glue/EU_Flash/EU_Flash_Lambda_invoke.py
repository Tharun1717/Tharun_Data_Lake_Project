import boto3
import os

s3 = boto3.client('s3')
bucket_name = 'i3pl-xml-bronze-dev'
prefix = 'Flash_EU/'


def lambda_handler(event, context):
    # List all objects within the specific prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in response:
        return {
            'statusCode': 200,
            'body': 'No files to process'
        }

    for item in response['Contents']:
        key = item['Key']

        # Skip if file is in a subfolder (Processed_files/ or Raw_files/)
        if 'Processed_files/' in key or 'Raw_files/' in key:
            continue

        filename = os.path.basename(key)

        # Verify that file is a CSV and matches the name structure
        if filename.endswith('.csv') and '_' in filename:
            parts = filename.split('_')
            if len(parts) == 4:
                date, flash, type_, site = parts
                site = os.path.splitext(site)[0]  # remove '.csv' extension from site

                year = date[:4]
                month = date[4:6]
                day = date[6:8]

                # Define new key based on conditions
                if "Dynamics" in filename:
                    new_key = f'Flash_EU/Raw_files/Dynamics/{year}/{month}/{day}/{filename}'
                else:
                    new_key = f'Flash_EU/Raw_files/{site}/{year}/{month}/{day}/{type_}/{filename}'

                # Check if the new key is not the same as the original key
                if new_key != key:
                    try:
                        # Copy object to new location
                        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': key}, Key=new_key)

                        # Delete the original object
                        s3.delete_object(Bucket=bucket_name, Key=key)
                    except Exception as e:
                        print(f'Error processing file {filename}: {str(e)}')
        else:
            print(f'Skipping file {filename} as it does not match the expected naming structure or is not a CSV file')

    return {
        'statusCode': 200,
        'body': 'All files processed'
    }
