import boto3
import os

s3 = boto3.client('s3')
bucket_name = 'i3pl-xml-bronze-uat'
prefix = 'fixed_commitments/'

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
        
        # Skip if file is in a subfolder (loaded/ or Raw_files/)
        if 'loaded/' in key or 'Raw_files/' in key:
            continue

        filename = os.path.basename(key)

        # Verify that file is a xlsx and matches the name structure
        if filename.endswith('.xlsx') and '-' in filename and '.' in filename:
            parts = filename.split('-')
            if len(parts) == 3:
                month_year, day, description = parts

                # Replace spaces with underscores and remove extra spaces
                month_year = ' '.join(month_year.split())
                description = os.path.splitext(description)[0].strip().replace(' ', '_')

                # Split month_year into year and month
                year, month = month_year.rsplit(' ', 1)

                # Define new key based on conditions
                new_key = f'fixed_commitments/Raw_files/{year}/{month}/{day}/{description}/{filename}'

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
            print(f'Skipping file {filename} as it does not match the expected naming structure or is not a xlsx file')

    return {
        'statusCode': 200,
        'body': 'All files processed'
    }
