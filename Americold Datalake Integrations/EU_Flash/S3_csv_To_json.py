import boto3
import pandas as pd
from io import StringIO
import logging
import datetime
import gzip

# Initialize S3 client
s3 = boto3.client('s3')

# Define S3 bucket and prefix
bucket_name = 'i3pl-xml-bronze-dev'
input_bucket = "i3pl-xml-bronze-dev"
output_bucket = 'i3pl-json-silver-dev'
prefix = 'Flash_EU/Raw_files/'

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3_bucket = s3_resource.Bucket(input_bucket)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_all_s3_keys(s3_bucket, prefix):
    """Get a list of all keys in an S3 bucket."""
    keys = []
    try:
        for file in s3_bucket.objects.filter(Prefix=prefix):
            keys.append(file.key)
    except Exception as e:
        logger.error("Failed to retrieve keys from S3 bucket: %s", e)
        return []
    return keys


def filter_files(file_list):
    """Filter files based on file type and prefix."""
    inv_files = [file for file in file_list if file.endswith(".csv") and "_INV_" in file]
    in_files = [file for file in file_list if file.endswith(".csv") and "_IN_" in file]
    out_files = [file for file in file_list if file.endswith(".csv") and "_OUT_" in file]
    return inv_files, in_files, out_files


def read_s3_csv(files, bucket_name):
    """Read CSV files from S3 and return a list of DataFrames."""
    dataframes = []
    for file in files:
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('latin1')), low_memory=False)
            dataframes.append(df)
        except Exception as e:
            logger.error("Failed to read CSV from S3: %s", e)
    return dataframes


def process_df(df, inv_df=False, column_map={}):
    """Process dataframe by renaming columns and converting datetime."""
    df.rename(columns=lambda x: x.lower(), inplace=True)
    df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    if inv_df:
        df.rename(columns=column_map, inplace=True)

    return df


def dump_to_s3(df, bucket):
    """Dump dataframe to S3 bucket as a JSON."""
    try:
        # Get today's date
        now = datetime.datetime.now()
        today = datetime.date.today()
        year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")
        date_str = now.strftime("%Y%m%d%H%m%s")

        # Convert the DataFrame to a JSON string
        json_str = df.to_json(orient='records', lines=True)

        # Compress the JSON string
        compressed_data = gzip.compress(json_str.encode())

        # Write the compressed data to S3
        s3.put_object(Body=compressed_data, Bucket=output_bucket,
                      Key=f"EU_FLASH_CONSOLIDATED/{year}/{month}/{day}/eu_flash_consolidated_{date_str}.json.gz")
    except Exception as e:
        logger.error("Failed to dump dataframe to S3: %s", e)


def move_files(bucket, file_list):
    """Move files to Processed_files folder after processing."""
    try:
        for file in file_list:
            copy_source = {'Bucket': bucket, 'Key': file}
            new_key = file.replace('Raw_files', 'Processed_files')
            s3.copy(copy_source, bucket, new_key)
            s3.delete_object(Bucket=bucket, Key=file)
    except Exception as e:
        logger.error("Failed to move files: %s", e)


def process_consolidated_df(df):
    """Final process for the consolidated dataframe."""
    try:
        # Reorder Columns
        column_order = ['report_date', 'facility_id', 'customer_id', 'sap_customer_id',
                        'item_code', 'document_no', 'receipt_date', 'lpn', 'pallet_type', 'current_pallets',
                        'current_cases', 'current_gross_weight', 'inbound_pallets',
                        'inbound_cases', 'inbound_gross_weight', 'outbound_pallets',
                        'outbound_cases', 'outbound_gross_weight', 'current_gross_weight_uom', 'uom',
                        'room', 'wms_address', 'temp']

        df = df[column_order]

        # Fill NA's for metric columns
        columns_to_fill = ['current_pallets', 'current_cases', 'current_gross_weight',
                           'inbound_pallets', 'inbound_cases', 'inbound_gross_weight',
                           'outbound_pallets', 'outbound_cases', 'outbound_gross_weight']

        df[columns_to_fill] = df[columns_to_fill].fillna(0)
    except Exception as e:
        logger.error("Failed to process output file: %s", e)
    return df


def fix_comma_floats(df):
    try:
        for col in ['current_pallets', 'current_cases', 'current_gross_weight',
                    'inbound_cases', 'inbound_gross_weight', 'outbound_cases', 'outbound_gross_weight']:
            df[col] = df[col].astype("str").str.replace(",", ".", regex=False)
        return df
    except Exception as e:
        logger.error("Failed to fix comma from file: %s", e)


def match_schema(df):
    """Matching schema from Redshift table."""
    try:
        df = df.astype({
            'report_date': 'datetime64[ns]',
            'facility_id': 'string',
            'customer_id': 'string',
            'sap_customer_id': 'string',
            'item_code': 'string',
            'document_no': 'string',
            'receipt_date': 'datetime64[ns]',
            'lpn': 'string',
            'pallet_type': 'string',
            'current_pallets': 'float64',
            'current_cases': 'float64',
            'current_gross_weight': 'float64',
            'inbound_pallets': 'int64',
            'inbound_cases': 'float64',
            'inbound_gross_weight': 'float64',
            'outbound_pallets': 'int64',
            'outbound_cases': 'float64',
            'outbound_gross_weight': 'float64',
            'current_gross_weight_uom': 'string',
            'uom': 'string',
            'room': 'string',
            'wms_address': 'string',
            'temp': 'string'
        })

        # Convert datetime fields to string in the format compatible with Redshift
        df['report_date'] = df['report_date'].dt.strftime('%Y-%m-%d')
        df['receipt_date'] = df['receipt_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error("Failed to match schema: %s", e)
    return df


batch_size = 100000  # You can adjust this value based on your available memory


def process_large_dataframe(df):
    """Process a large DataFrame in smaller batches."""
    num_batches = len(df) // batch_size + 1

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size
        batch_df = df[start_idx:end_idx]

        # Process the batch as needed
        batch_df = process_consolidated_df(batch_df)
        batch_df = fix_comma_floats(batch_df)
        batch_df = match_schema(batch_df)

        # Export the batch to S3
        dump_to_s3(batch_df, output_bucket)


def main():
    """Main function to run the script."""
    try:
        logger.info("Step 1: Getting files from bronze layer.")
        file_list = get_all_s3_keys(s3_bucket, "Flash_EU/Raw_files/")
        inv_files, in_files, out_files = filter_files(file_list)

        logger.info(f"We have {len(inv_files)} INV files.")
        logger.info(f"We have {len(in_files)} IN files.")
        logger.info(f"We have {len(out_files)} OUT files.")

        logger.info("Read csvs.")
        inv_dataframes = read_s3_csv(inv_files, bucket_name)
        in_dataframes = read_s3_csv(in_files, bucket_name)
        out_dataframes = read_s3_csv(out_files, bucket_name)

        logger.info("Step 2: Processing dataframes")
        if len(inv_dataframes) > 0:
            df_inv = pd.concat(inv_dataframes, ignore_index=True)
            inv_column_rename = {'uoms_weight_uom': 'current_gross_weight_uom'}
            df_inv = process_df(df_inv, inv_df=True, column_map=inv_column_rename)
        else:
            df_inv = pd.DataFrame()
        if len(in_dataframes) > 0:
            df_in = pd.concat(in_dataframes, ignore_index=True)
            df_in = process_df(df_in)
        else:
            df_in = pd.DataFrame()
        if len(out_dataframes) > 0:
            df_out = pd.concat(out_dataframes, ignore_index=True)
            df_out = process_df(df_out)
        else:
            df_out = pd.DataFrame()

        logger.info("Step 4: Creating final output.")
        eu_flash_detail = pd.concat([df_inv, df_in, df_out])

        logger.info("Step 5: Final process for the eu_flash_detail df.")
        # Process the large DataFrame in smaller batches
        process_large_dataframe(eu_flash_detail)

        logger.info("Step 8: Moving processed files to Processed folder.")
        # Call the function to move files
        move_files(bucket_name, file_list)

        logger.info("Finishing process...")

    except Exception as e:
        logger.error("Failed to run the script: %s", e)


if __name__ == "__main__":
    main()
