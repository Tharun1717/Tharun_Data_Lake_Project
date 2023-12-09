import gzip, logging
# import pandas as pd
# import numpy as np
import xmltodict
from io import BytesIO
import xml.etree.ElementTree as ET
from src.constants import MONTH_DICT, FILTER_FOLDER, INPUT_BUCKET, S3_OUTPUT_PATH, TOPIC_ARN, XML_LOG_BUCKET, KEYS_DICT
import sys
import datetime
import json
import pytz


# ====================================================
#              Logger function definition
# ====================================================

def set_logger(logger_name, log_path, mode="w"):
    """
    The purpose of this function is to create a logger file to trace the state of the job.

    Parameters:
    logger_name (str): name of the logger.
    log_path (str): path where the logger will be written.
    mode (str, optional): mode in which the log file will be opened (default is "w").

    Returns:
    logging.Logger: The logging object for the specified logger.
    """

    logging.basicConfig(
        filename=log_path,
        # stream=sys.stdout,
        filemode=mode,
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s ",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )
    logger = logging.getLogger(logger_name)
    return logger


# ====================================================
#              Function definition
# ====================================================


def get_file(file, s3_client, bucket=INPUT_BUCKET, decoder=False,
             encodings=['utf-8', 'windows-1252', 'latin1', 'utf-16']):
    """
    The objective of this auxiliary function is to get a file from an s3 bucket.

    Params:
    :file: XML file that is going to be parsed.
    :s3_client: s3 boto3 client to grab the file.
    :bucket: path inside the bucket where the function will grab the file.
    :decoder: Flag to indicate whether to try multiple encodings.
              Default = False.
    :encodings: List of encodings to try when decoding the file.
                Default = ['utf-8', 'windows-1252', 'latin1', 'utf-16']
    """

    object = s3_client.get_object(Bucket=bucket, Key=file)
    xml_string = object['Body'].read()

    if ".gz" in file:
        xml_string = gzip.decompress(xml_string)

    if decoder == True:
        # If decoder flag is True, try multiple encodings
        last_exception = None
        for encoding in encodings:
            try:
                decoded_xml = xml_string.decode(encoding)
                return BytesIO(decoded_xml.encode('utf-8'))
            except UnicodeDecodeError as e:
                last_exception = e
                continue
        # If none of the encodings worked, raise the last exception encountered.
        raise last_exception
    else:
        xml = BytesIO(xml_string)

    return xml


def dump_s3(df, path, filename, logger, S3_OUTPUT_PATH=S3_OUTPUT_PATH):
    '''
    The purpose of this function is to dump the parsed JSON data into the silver layer.

    Parameters:
    :df: Pandas DataFrame that represents the parsed XML file. It will be dumped as a JSON file in the silver layer.
    :path: Path to the silver layer, obtained using the `obtain_path_filename` function.
    :filename: Filename, obtained using the `obtain_path_filename` function.
    :logger: Logger instance to log errors.
    :S3_OUTPUT_PATH: Output path for the JSON file in the silver layer.

    Returns:
    None
    '''

    try:
        df.to_json(f"{S3_OUTPUT_PATH}/{path}{filename}.json", orient='records', lines=True)

    except Exception as ex:
        logger.info(f"Error in dump_s3 - {ex}")
        raise ex


def load_xml_list(s3_bucket, logger, chunksize, filetypes, FILTER_FOLDER=FILTER_FOLDER):
    """
    The main purpose of this function is to grab the list of the XML files that are going to be parsed in each run.

    Parameters:
    - s3_bucket (boto3.resources.factory.s3.Bucket): Input bucket which contains the XML files that we are going to parse.
    - logger: logger object.
    - chunksize (int): Integer representing the amount of XML files that we want to parse in each run.
        According to the documentation, the desired chunksize will be 10,000 files per run, plus an additional 5 snapshot files.
    - filetypes (str): filetypes of files that will be parsed. This value will always be "all".
    - FILTER_FOLDER (str, optional): Folder inside the input bucket that contains the files that we want to parse. It will be "Processed/" folder by default.

    Returns:
    - list: A list of XML file names.
    """

    xml_files = []
    modified = []

    try:
        i = 0
        for file in s3_bucket.objects.filter(Prefix=FILTER_FOLDER):
            name = file.key
            if filetypes == "all" and name != 'Processed/':
                name = file.key
                time = file.last_modified
                xml_files.append(name)
                modified.append(time)

            i += 1
            if i == 100000:
                break

        dictionary = dict(zip(xml_files, modified))

        xml_files_sorted = list(dict(sorted(dictionary.items(), key=lambda item: item[1])).keys())

        snapshot_files = [file for file in xml_files_sorted if "SNAPSHOT" in file.upper()]

        xml_files = [file for file in xml_files_sorted if "SNAPSHOT" not in file.upper()]

        if len(xml_files) > chunksize:
            xml_files = xml_files[:chunksize]
        if len(snapshot_files) >= 5:
            xml_files += snapshot_files[:5]
        elif len(snapshot_files) < 5:
            xml_files += snapshot_files[:len(snapshot_files)]
        elif len(snapshot_files) == 0:
            xml_files = xml_files

    except Exception as ex:
        logger.exception(f"Error loading xml lists - {ex}")
        raise ex

    return xml_files


def move_xml_files(s3_resource, input_bucket, file, type, logger):
    '''
    The objective of this function is to move files from one folder to another in s3.
    Scenarios when we move files:
        - After parsing: After we parse each file, it will be moved from Processed folder to Parsed folder inside the input bucket. This avoids reprocessing the same
        file twice. --> This type is called "parsed".
        - When we found an error: when the process found an error in a file and cannot be parsed, the file will be moved from Processed to Parsing_error folder inside
        the input bucket. --> This type is called "problem".
        - When we encounter a filetype that we don't know: If the filtype is not one of the one that are known by us, we move it to the Unknown_filetype folder in the
        bronze layer.


    Params:
    :s3_resource: (boto3.resources.factory.s3.Bucket): Input bucket which contains the XML files.
    :input_bucket: Input bucket from S3.
    :file (str): path/file that we want to move (with it's path - name key).
    :type (str): "parsed",  "problem" or "unknown_filetype". Scenarios explained above.
    :logger: logger.

    Returns:
    None


    '''

    try:
        if type == "parsed":
            # Copying file and pasting it in Error folder in input bucket
            s3_resource.Object(input_bucket, f'{file.replace("Processed", "Parsed")}').copy_from(
                CopySource=input_bucket + '/' + file)
            # Deleting file from Processed bucket.
            s3_resource.Object(input_bucket, file).delete()
        elif type == "unknown_filetype":
            # Copying file and pasting it in Error folder in input bucket
            s3_resource.Object(input_bucket, f'{file.replace("Processed", "Unknown_filetype")}').copy_from(
                CopySource=input_bucket + '/' + file)
            # Deleting file from Processed bucket.
            s3_resource.Object(input_bucket, file).delete()
        elif type == "problem":
            # Copying file and pasting it in Error folder in input bucket
            s3_resource.Object(input_bucket, f'{file.replace("Processed", "Parsing_error")}').copy_from(
                CopySource=input_bucket + '/' + file)
            # Deleting file from Processed bucket.
            s3_resource.Object(input_bucket, file).delete()
        else:
            logger.info(f"type = {type} not recognized.")

    except Exception as ex:
        logger.exception(f"Error in move_parsed_files - {ex}")
        raise ex


def email_notification(sns, problem_files, unknown_filetypes, type, logger, TOPIC_ARN=TOPIC_ARN):
    '''
    The purpose of this function is to send a notification email for files that cannot be parsed for any reason. Files that cannot be parsed, will be moved
    with the move_files function, and an email will be sent to notify this to the corresponding users.


    '''

    try:

        # Creating body of the email depending on some statements
        if type == "problem":

            subject = f"PARSING XML - PROBLEM FILES"

            result_str = ""

            for key, value in problem_files.items():
                result_str += f"- File {str(key).split('/')[-1]}\n"
                result_str += f"     - Path: {str(key).replace('Processed', 'Parsing_error')}\n"
                result_str += f"     - Error: {value}\n\n"
                result_str += f"=================================\n\n"

            text = f"""

The cause of the files not being parsed could be:
- File contained an invalid token or the file was broken.
- XML tags were not correctly specified (unclosed tags)
- File contained not valid filename.
- File has no valid date in its path.
- XML file was empty.

Please note that the following files could not be parsed:

{result_str}

Note: This files were moved to Parsing_error folder in the bronze layer. Please check the path of each file in the list above.

        """

        elif type == "unknown_filetype":

            subject = "PARSING XML - UNKNOWN FILETYPES"

            result_str = ""

            for file in unknown_filetypes:
                result_str += f"- File {str(file).split('/')[-1]}\n"
                result_str += f"     - Path: {str(file).replace('Processed', 'Unknown_filetype')}\n"

            text = f"""
Please note that the following files were not parsed because they are from a filetype that it is unknown at the moment:

{result_str}

Note: This files were moved to Unknown_filetype folder in the bronze layer. Please check the path of each file in the list above.     
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
        logger.exception(f"Error in send_email_notification {ex}")
        raise ex


def obtain_file_category(file):
    filetype = file.split("/")[4]
    filename = file.split("/")[-1]

    if 'DOCUMENT' in filename.upper():
        category = "DOCUMENT"

    elif "TRANSACTION" in filename.upper():
        category = "TRANSACTION"

    elif filetype.upper() == "APPOINTMENT":
        category = "APPOINTMENT"

    elif filetype.upper() in ['ADJUSTMENT', "INVENTORY_ADJUSTMENT"]:
        category = "ADJUSTMENT"

    elif filetype.upper() in ['MANUAL_KPI', "MANUALKPI"]:
        category = "MANUALKPI"

    elif filetype.upper() in ["GLOBAL_CONSIGNEES", "GLOBAL_CONSIGNEE", "CONSIGNEE", "CONSIGNEES", "CARRIER", "CUSTOMER",
                              "CUSTOMERS", "VENDOR", "TRADING_PARTNERS", "TRADING_PARTNER"]:
        category = "TRADING_PARTNERS"

    elif filetype.upper() == "SNAPSHOT":
        category = "SNAPSHOT"

    elif filetype.upper() == "ITEMINFO":
        category = "ITEMINFO"

    elif filetype.upper() == "APAPPOINTMENT":
        category = "APAPPOINTMENT"

    else:
        category = filetype

    return category


# def aux_xml_audit(file, status, MONTH_DICT = MONTH_DICT, XML_LOG_BUCKET=XML_LOG_BUCKET):

#     '''
#     The purpose of this auxiliary function is to create the dataframe with the required fields to keep track of the XML files that have been parsed.

#     Fields that are created:
#         - wms: wms system corresponding to the file.
#         - site: site corresponding to the file.
#         - filetype: filetype of the file.
#         - filename: filename of the file.
#         - file_date: date that appears in the filename.
#         - parsing_date: date when the script was run to parse the file.
#         - status: PARSED for parsed files; PROBLEM for error files; UNKNOWN TYPE for unknown filetypes.


#     Params:
#     :file_list: list of files that are going to be transformed by the function.
#         - It could be parsed_files, problem_files or unknown_filetypes. This would be applied in the function xml_audit_log_file
#     :status: for parsed_files it will be PARSED; for problem_files it will be PROBLEM; for unknown_filetypes it will be UNKNOWN FILETYPE
#     '''

#     try:
#         file_list = [file]
#         df = pd.DataFrame()
#         if len(file_list) > 0:

#             #Obtain todays date:
#             today = str(datetime.datetime.today()).split(" ")[0].replace("-", "")

#             #Path of the file
#             df["path"] = pd.Series(file_list).str.replace("Processed/", "")

#             #Obtaining date values
#             df["year"] = df["path"].apply(lambda x: x.split("/")[2].split("-")[0])
#             df["month"] = df["path"].apply(lambda x: MONTH_DICT[x.split("/")[2].split("-")[1]])
#             df["day"] = df["path"].apply(lambda x: x.split("/")[3].split("-")[0])


#             #WMS of the file
#             df["wms"] = df["path"].apply(lambda x: x.split("/")[0])
#             #Site of the file
#             df["site"] = df["path"].apply(lambda x: x.split("/")[1])
#             #Type of file
#             df["filetype"] = df["path"].apply(lambda x: x.split("/")[4].upper())
#             #Category
#             df["filetype_category"] = df["path"].apply(lambda x: obtain_file_category(x))
#             #Filename
#             df["filename"] = df["path"].apply(lambda x: x.split("/")[-1].split(".")[0])
#             #Date in the file
#             df["file_date"] = (df["year"] +  df["month"] + df["day"])
#             #Parsing date
#             df["parsing_date"] = today
#             #Status
#             df["status"] = status


#             #Final DF
#             df = df[["wms",
#             "site",
#             "filetype",
#             "filetype_category",
#             "filename",
#             "file_date",
#             "parsing_date",
#             "status"
#             ]]


#             # Obtain values for path
#             year = today[:4]
#             month = today[4:6]
#             day = today[6:]

#             path = f"{XML_LOG_BUCKET}{year}/{month}/{day}/"

#     except Exception as e:
#         raise e

#     return df, path


def convert_xml_to_json(file, logger, s3_client, bucket=INPUT_BUCKET, decoder=False):
    """
    This function converts an XML file stored in an S3 bucket to a JSON structure.

    The function takes in an XML, an instance of a logger, an S3 client, an optional S3 bucket name, and
    an optional decoder. It retrieves the file from the specified S3 bucket, converts the XML data to a dictionary,
    then transforms all keys to lowercase, removes '@' from keys, and eliminates 'urn:i3pl:' from keys. The
    function returns the transformed data dictionary. If any exception occurs during the process, the function
    logs the error message and re-raises the exception.

    Parameters:
    - file (str): The name of the XML file to be converted.
    - logger (logging.Logger): An instance of a logger, to log any errors during the conversion process.
    - s3_client (boto3.client): The Boto3 S3 client, to interact with Amazon S3.
    - bucket (str, optional): The name of the S3 bucket where the XML file is stored. Defaults to the value of INPUT_BUCKET.
    - decoder (bool, optional): A flag indicating whether to use a decoder while reading the file. Defaults to False.

    Returns:
    - data_dict (dict): The dictionary representing the JSON structure of the XML file.

    Raises:
    - Exception: If an error occurs during the conversion process, the function will log the error and re-raise the exception.
    """
    try:
        xml = get_file(file, s3_client, bucket, decoder)
        data_dict = xmltodict.parse(xml, process_namespaces=False)

        # recursive function to change all keys to lowercase, remove '@', and eliminate "urn:i3pl:" from keys
        def transform(d):
            if isinstance(d, list):
                return [transform(v) for v in d]
            elif isinstance(d, dict):
                return {k.lower().replace('@', '').replace('urn:i3pl:', ''): transform(v)
                        for k, v in d.items() if v != {"xsi:nil": "true"}}
            return d

        # perform transformation
        data_dict = transform(data_dict)
    except Exception as ex:
        logger.info(f"Error in parse function - {ex}")
        raise ex

    return data_dict


def remove_nil(d, logger):
    """
    This function recursively traverses through a dictionary or a list (generally derived from XML or JSON data),
    and replaces any dictionary of the form {"xsi:nil": "true"} with None.

    If the input data `d` is a dictionary, the function iterates over each key-value pair. If the value equals
    {"xsi:nil": "true"}, the corresponding key's value is set to None. If the value is another dictionary or a
    list, the function calls itself recursively to check the inner dictionary or list.

    If `d` is a list, the function iterates over each item and calls itself recursively to check each item.

    If an exception occurs during the process, it gets logged and re-raised.

    Parameters:
    - d (dict or list): The input data to be traversed. It should be a dictionary or a list.
    - logger (logging.Logger): An instance of a logger, to log any errors during the process.

    Raises:
    - Exception: If an error occurs during the process, the function will log the error and re-raise the exception.
    """
    try:
        if isinstance(d, dict):
            for k, v in list(d.items()):  # Make a copy with list(d.items()) for iteration
                if v == {"xsi:nil": "true"}:
                    d[k] = None  # Delete the key-value pair
                else:
                    remove_nil(v, logger)  # Recursively check inner values
        elif isinstance(d, list):
            for item in d:
                remove_nil(item, logger)  # Recursively check list items

    except Exception as ex:
        logger.exception(f"Error in remove_nil - {ex}")
        raise ex


def classify_file(filetype_check, logger):
    """
    This function classifies a file based on the input filetype_check string.

    It checks the value of filetype_check against a list of known file types and returns a standardized
    classification string according to its category. If the filetype_check string does not match any known type,
    it returns "UNKNOWN". The function uses a series of if/elif conditions to perform the check and map the
    filetype_check to a category.

    If an exception occurs during the process, the error message gets logged and the exception is re-raised.

    Parameters:
    - filetype_check (str): The string to be checked against the known file types.
    - logger (logging.Logger): An instance of a logger, to log any errors during the process.

    Returns:
    - str: The category of the file type.

    Raises:
    - Exception: If an error occurs during the process, the function will log the error and re-raise the exception.
    """
    try:
        if filetype_check in ['DOCUMENT', 'DOCUMENTS', 'TRANSACTION', 'TRANSACTIONS']:
            return "DOCUMENT"

        elif filetype_check == 'APPOINTMENT':
            return "APPOINTMENT"

        elif filetype_check in ['ADJUSTMENT', "INVENTORY_ADJUSTMENT"]:
            return "ADJUSTMENT"

        elif filetype_check in ['MANUAL_KPI', "MANUALKPI"]:
            return "MANUAL_KPI"

        elif filetype_check in ["GLOBAL_CONSIGNEES", "GLOBAL_CONSIGNEE", "CONSIGNEE", "CONSIGNEES", "CARRIER",
                                "CUSTOMER",
                                "CUSTOMERS", "VENDOR", "TRADING_PARTNERS", "TRADING_PARTNER"]:
            return "TRADING_PARTNERS"

        elif filetype_check == 'SNAPSHOT':
            return "SNAPSHOT"

        elif filetype_check == 'ITEMINFO':
            return "ITEMINFO"

        elif filetype_check == 'APAPPOINTMENT':
            return "APAPPOINTMENT"

        elif filetype_check in ["LOCATIONINFO", "LOCATIONMASTER", "LOCATION_INFO", "LOCATION_MASTER"]:
            return "LOCATION_MASTER"

        else:
            return "UNKNOWN"
    except Exception as ex:
        logger.exception(f"Error in classify_file - {ex}")
        raise ex


def check_json_size(data, logger):
    """
    This function checks if the size of the input data when converted to a JSON string exceeds 1 Megabyte.

    It works by first converting the input dictionary into a JSON string, then calculating the size of this string in bytes,
    and finally converting this size into Megabytes. If the size in Megabytes is greater than or equal to 1, it returns True.
    If it's less than 1, it returns False.

    If an exception occurs during the process, it gets logged and the exception is re-raised.

    Parameters:
    - data (dict): The input dictionary to be checked.
    - logger (logging.Logger): An instance of a logger, to log any errors during the process.

    Returns:
    - bool: True if the size of the input data as a JSON string is greater than or equal to 1 Megabyte, False otherwise.

    Raises:
    - Exception: If an error occurs during the process, the function will log the error and re-raise the exception.
    """

    try:

        # Convert the dictionary to a JSON string
        json_str = json.dumps(data)

        # Get the size of the string in bytes
        size_in_bytes = sys.getsizeof(json_str)

        # Convert the size from bytes to megabytes
        size_in_mb = size_in_bytes / (1024 * 1024)

        # If size_in_mb is greater than or equal to 1, return True. Else, return False
        if size_in_mb >= 1:
            return True
        else:
            return False
    except Exception as ex:
        logger.exception(f"Error in check_json_data - {ex}")
        raise ex


def obtain_file_information(file, logger):
    """
    This function extracts valuable information from a given file path.

    The extracted information includes:
    - facility_id: Extracted from the 3rd segment of the file path (assuming the file path is separated by '/').
    - wms: Extracted from the 2nd segment of the file path.
    - filename: Extracted from the last segment of the file path without the file extension.
    - parsing_date: The current date formatted as 'YYYYMMDD'.
    - filetype_check: Extracted from the 6th segment of the file path.
    - filetype: The classified file type based on 'filetype_check' (classification is done by the classify_file function).
    - path: A newly generated path with the structure '{filetype}/{Year}/{Month}/{Day}/'.

    If an exception occurs during the process, it gets logged and the exception is re-raised.

    Parameters:
    - file (str): The input file path from which information is to be extracted.
    - logger (logging.Logger): An instance of a logger, to log any errors during the process.

    Returns:
    - tuple: A tuple of strings representing the extracted file information (facility_id, wms, filename, parsing_date, filetype, path).

    Raises:
    - Exception: If an error occurs during the process, the function will log the error and re-raise the exception.
    """

    try:
        facility_id = file.split("/")[2]
        wms = file.split("/")[1]
        filename = file.split("/")[-1].split(".")[0]
        # Ensuring EST timezone
        eastern_tz = pytz.timezone("America/New_York")
        parsing_date = datetime.datetime.now(eastern_tz).strftime("%Y%m%d")  # Get today's date
        if "LOCATION" in file:
            parsing_date = datetime.datetime.now(eastern_tz).strftime("%Y%m%d%H%M%S")
        filetype_check = file.split("/")[5]
        filetype = classify_file(filetype_check, logger)
        path = f'{filetype}/{parsing_date[:4]}/{parsing_date[4:6]}/{parsing_date[6:8]}/'

        return facility_id, wms, filename, parsing_date, filetype, path

    except Exception as ex:
        logger.exception(f"Error in obtain_file_information - {ex}")
        raise ex


def process_dict(d, filetype, logger, keys_dict=KEYS_DICT):
    """
    Function to remove specific values from dictionary and ensure specific keys in a nested
    dictionary are associated with a list.

    Parameters:
    - d: The input dictionary.
    - filetype: The filetype to check. Determines which keys to look for based on keys_dict.
    - keys_dict: A dictionary mapping filetypes to lists of keys to check.

    Returns: The input dictionary with the specified keys ensured to be associated with a list
    and special values replaced with None.
    """
    try:
        keys = keys_dict.get(filetype, [])

        def ensure_lists(d, parent_key=None):
            if isinstance(d, dict):
                for k, v in list(d.items()):  # Make a copy of items as we might modify the dict
                    if k in keys and (parent_key != k or isinstance(v, dict)):
                        d[k] = [v] if not isinstance(v, list) else v
                    ensure_lists(v, k)
            elif isinstance(d, list):
                for item in d:
                    ensure_lists(item, parent_key)

        def insert_missing_keys(d):
            if isinstance(d, list):
                for item in d:
                    insert_missing_keys(item)
            elif isinstance(d, dict):
                for k, v in d.items():
                    # Add new condition for filetype DOCUMENT
                    if filetype == "DOCUMENT":
                        # Check for document_notes and add if not present
                        if k == "document":
                            for doc in v:
                                if "document_notes" not in doc or doc.get("document_notes") is None:
                                    doc["document_notes"] = {"document_note": [{}]}
                                if "document_lines" not in doc or doc.get("document_lines") is None:
                                    doc["document_lines"] = {"document_line": [{"line_dtls": {"inventory": [{}]}}]}
                                # if "pallet" not in doc or doc.get("pallet") is None:
                                #     doc["pallet"] = [{}]

                        # Insert new dictionary
                        if k == "document_line" and isinstance(v, list):  # Ensure that "document_line" is a list
                            for doc in v:
                                if "line_dtls" not in doc or doc.get("line_dtls") is None:
                                    doc["line_dtls"] = {"inventory": [{}]}

                    # Add new condition for filetype APPOINTMENT
                    elif filetype == "APPOINTMENT":
                        # Insert new dictionary
                        if k == "appointment":
                            for appt in v:
                                if "appointment_notes" not in appt or appt.get("appointment_notes") is None:
                                    appt["appointment_notes"] = {"note": [{}]}

                    # Add new condition for filetype MANUALKPI
                    elif filetype == "MANUAL_KPI":
                        # Insert new dictionary for "kpi"
                        if k == "kpi" and isinstance(v, list):  # Ensure that "kpi" is a list
                            for kpi in v:
                                customer_details = kpi.get("customer_details")
                                if customer_details:  # If "customer_details" exist
                                    for detail in customer_details:
                                        current = detail.get("current")
                                        if current is None:  # If "current" doesn't exist, create it
                                            detail["current"] = {"pallet_occupancy_details": [{}]}
                                        elif "pallet_occupancy_details" not in current:  # If "pallet_occupancy_details" doesn't exist under "current"
                                            current["pallet_occupancy_details"] = [{}]

                    # Add new condition for lOCATION MASTER
                    elif filetype == "LOCATION_MASTER" and 'location_master' in d:
                        if 'uoms' not in d['location_master']:
                            d['location_master']['uoms'] = {}

                        # Set default values
                        d['location_master']['uoms'].setdefault("date_timezone", "America/New_York")
                        d['location_master']['uoms'].setdefault("weight_uom", "LB")
                        d['location_master']['uoms'].setdefault("dimension_uom", "IN")
                        d['location_master']['uoms'].setdefault("volume_uom", "IN3")
                        d['location_master']['uoms'].setdefault("temperature_uom", "F")

                    # Continue recursively checking inner values
                    insert_missing_keys(v)

        # First operation
        ensure_lists(d)

        # Second operation
        insert_missing_keys(d)

        return d
    except Exception as ex:
        logger.exception(f"Error in process_dict - {ex}")
        raise ex

    # def split_and_upload(data, chunk, chunk_size, filename, path, OUTPUT_BUCKET, s3_client, logger, filetype, one_document=False, level=1, sub_level_index=''):
    #     """
    #     This is a helper recursive function to split a chunk if it is too large, and upload it to S3.

    #     Parameters:
    #     - data (dict): The input dictionary to be checked.
    #     - chunk (list): The chunk of data to check.
    #     - chunk_size (int): The size of chunks that data is being split into.
    #     - filename (str): The base filename to be used for the split JSON files.
    #     - path (str): The path to the location where the split JSON files will be stored.
    #     - OUTPUT_BUCKET (str): The name of the S3 bucket where the split JSON files will be stored.
    #     - s3_client (boto3.client): The S3 client used to interact with AWS S3.
    #     - logger (logging.Logger): An instance of a logger, to log any errors during the process.
    #     - filetype (str): The type of the file, used to identify the list to be split in the JSON data.
    #     - one_document (bool): Flag to identify if there's only one document in the file. Default is False.
    #     - level (int): The current level of recursion.
    #     - sub_level_index (str): The sub level index to be appended to the filename. Default is empty string.

    #     Returns:
    #     - None
    #     """

    #     # Replace the split list in the original data with the current chunk.
    #     if filetype == "SNAPSHOT":
    #         data['xml_data']['snapshot']['inventory_list']['inventory'] = chunk
    #     elif filetype == "DOCUMENT":
    #         if one_document:
    #             data['xml_data']['document_list']['document'][0]['document_lines']['document_line'] = chunk
    #         else:
    #             data['xml_data']['document_list']['document'] = chunk
    #     elif filetype == "APPOINTMENT":
    #         data['xml_data']['appointment_list']['appointment'] = chunk
    #     elif filetype == "TRADING_PARTNERS":
    #         data['xml_data']['trading_partner_list']['trading_partner'] = chunk
    #     elif filetype == "ITEMINFO":
    #         data['xml_data']['iteminfo_list']['iteminfo'] = chunk
    #     elif filetype == "MANUAL_KPI":
    #         data['xml_data']['manual_kpi']['kpi_list']['kpi'] = chunk
    #     elif filetype == "ADJUSTMENT":
    #         data['xml_data']['inventory_adjustment']['in']['inventory'] = chunk

    #     # Add an additional level to the filename to reflect the nested chunk.
    #     data["filename_chunk"] = f"{filename}_{sub_level_index}"

    #     # Convert the nested chunk to a JSON string.
    #     json_string = json.dumps(data)

    #     # Check if the size of the json_string is greater than 0.9 MB.
    #     if check_json_size(json.loads(json_string), logger) and level < 10:  # Add a limit to the level of splitting.
    #         # If the chunk is greater than 0.5 MB, we split it further into nested chunks.
    #         nested_chunks = [chunk[i:i + max(chunk_size//2, 1)] for i in range(0, len(chunk), max(chunk_size//2, 1))]

    #         # Now we loop over each nested chunk and save it as a separate file.
    #         for i, nested_chunk in enumerate(nested_chunks):
    #             split_and_upload(data, nested_chunk, max(chunk_size//2, 1), filename, path, OUTPUT_BUCKET, s3_client, logger, filetype, one_document, level+1, f"{sub_level_index}{i+1}_")
    #     else:
    #         # If the chunk is not greater than 0.9 MB, or the maximum level of splitting is reached,
    #         # we compress the string and upload it to the S3 bucket.
    #         compressed_data = gzip.compress(json_string.encode())
    #         s3_client.put_object(Body=compressed_data, Bucket=OUTPUT_BUCKET, Key=f"{path}{filename}_{sub_level_index}.json.gz")


def split_and_upload(data, chunk, chunk_size, filename, path, OUTPUT_BUCKET, s3_client, logger, filetype,
                     one_document=False, one_line=False, level=1, sub_level_index=''):
    """
    This is a helper recursive function designed to split a chunk of data if its size exceeds a defined limit and then upload it to S3. The function handles various filetypes, and can split the data based on different nested structures within the JSON document.

    Parameters:
    - data (dict): The input dictionary that contains the entire data. Modified during recursion to contain only current chunk.
    - chunk (list): The portion of data that is being checked and possibly uploaded.
    - chunk_size (int): The target size for each chunk of split data.
    - filename (str): The base filename used for saving split JSON files.
    - path (str): The path on S3 where the split JSON files will be stored.
    - OUTPUT_BUCKET (str): Name of the S3 bucket to store the split JSON files.
    - s3_client (boto3.client): AWS S3 client for handling upload operations.
    - logger (logging.Logger): Logger instance for tracking and reporting activities.
    - filetype (str): Type of the data file. Determines the structure of the data and how it should be split.
    - one_document (bool, optional): Flag indicating if there's a single document in the file. Default is False.
    - one_line (bool, optional): Flag indicating if there's a single line in a document. Useful for further granularity when splitting. Default is False.
    - level (int, optional): The current depth of recursion. Used to avoid over-splitting. Default is 1.
    - sub_level_index (str, optional): Indexing suffix for filenames to track chunks at various levels. Default is an empty string.

    Returns:
    - None

    The function operates recursively, and checks the size of the current chunk of data. If the size exceeds a specified limit, it splits the data further and calls itself with the smaller chunk. If the size is within the acceptable range, it uploads the chunk to S3.

    Depending on the filetype, the function identifies the correct list within the JSON data to split. When dealing with documents (`filetype == "DOCUMENT"`), further granularity is introduced by the flags `one_document` and `one_line`, which respectively control whether to split by individual documents or by individual lines within a document.
    """

    # Replace the split list in the original data with the current chunk.
    if filetype == "SNAPSHOT":
        data['xml_data']['snapshot']['inventory_list']['inventory'] = chunk
    elif filetype == "DOCUMENT":
        if one_document and one_line == False:
            data['xml_data']['document_list']['document'][0]['document_lines']['document_line'] = chunk
        elif one_line:
            data['xml_data']['document_list']['document'][0]['document_lines']['document_line'][0]['line_dtls'][
                'inventory'] = chunk
        else:
            data['xml_data']['document_list']['document'] = chunk

    elif filetype == "APPOINTMENT":
        data['xml_data']['appointment_list']['appointment'] = chunk
    elif filetype == "TRADING_PARTNERS":
        data['xml_data']['trading_partner_list']['trading_partner'] = chunk
    elif filetype == "ITEMINFO":
        data['xml_data']['iteminfo_list']['iteminfo'] = chunk
    elif filetype == "MANUAL_KPI":
        data['xml_data']['manual_kpi']['kpi_list']['kpi'] = chunk
    elif filetype == "ADJUSTMENT":
        data['xml_data']['inventory_adjustment']['in']['inventory'] = chunk
    elif filetype == "LOCATION_MASTER":
        data['xml_data']['location_master']['location_info'] = chunk

    # Add an additional level to the filename to reflect the nested chunk.
    data["filename_chunk"] = f"{filename}_{sub_level_index}"

    # Convert the nested chunk to a JSON string.
    json_string = json.dumps(data)

    # Check if the size of the json_string is greater than 0.9 MB.
    if check_json_size(json.loads(json_string), logger):
        # If the chunk is greater than 0.5 MB, we split it further into nested chunks.
        if level < 5:
            logger.info(
                f"Chunk exceeds limit and level is less than 5. Further splitting into subchunks at level {level + 1}.")
            nested_chunks, chunk_size = get_chunk_size(chunk, 800)

            # Now we loop over each nested chunk and save it as a separate file.
            for i, nested_chunk in enumerate(nested_chunks):
                split_and_upload(data, nested_chunk, max(chunk_size // 2, 1), filename, path, OUTPUT_BUCKET, s3_client,
                                 logger, filetype, one_document, one_line, level + 1, f"{sub_level_index}{i + 1}_")
        else:
            # If the maximum level of splitting is reached,
            # check if chunk has only one document and its size still exceeds the limit.
            if filetype == "DOCUMENT" and len(chunk) == 1:
                if not one_document and len(chunk[0]["document_lines"]["document_line"]) > 1:
                    logger.info(
                        f"Maximum splitting level reached with single document exceeding limit. Splitting by document_line.")
                    document_lines = chunk[0]['document_lines']['document_line']
                    nested_chunks, chunk_size = get_chunk_size(document_lines, 800)
                    for i, nested_chunk in enumerate(nested_chunks):
                        split_and_upload(data, nested_chunk, max(chunk_size // 2, 1), filename, path, OUTPUT_BUCKET,
                                         s3_client, logger, filetype, one_document=True, level=level + 1,
                                         sub_level_index=f"{sub_level_index}{i + 1}_")
                elif not one_line and len(chunk[0]) and len(chunk[0]["document_lines"]["document_line"]) == 1:
                    logger.info(f"Single document line exceeding limit. Splitting by inventory.")
                    inventory_items = chunk[0]["document_lines"]["document_line"][0]['line_dtls']['inventory']
                    nested_chunks, chunk_size = get_chunk_size(inventory_items, 800)
                    # nested_chunks = [inventory_items[i:i + max(chunk_size//2, 1)] for i in range(0, len(inventory_items), max(chunk_size//2, 1))]

                    for i, nested_chunk in enumerate(nested_chunks):
                        split_and_upload(data, nested_chunk, max(chunk_size // 2, 1), filename, path, OUTPUT_BUCKET,
                                         s3_client, logger, filetype, one_line=True, level=level + 1,
                                         sub_level_index=f"{sub_level_index}{i + 1}_")
                else:
                    logger.warning(
                        f"Maximum splitting level reached but chunk still exceeds limit. Uploading anyway. Filename: {filename}_{sub_level_index}.json")
                    compressed_data = gzip.compress(json_string.encode())
                    s3_client.put_object(Body=compressed_data, Bucket=OUTPUT_BUCKET,
                                         Key=f"{path}{filename}_{sub_level_index}.json.gz")

    else:
        # If the chunk is not greater than 0.9 MB, we compress the string and upload it to the S3 bucket.
        logger.info(f"Chunk size within limit. Uploading directly. Filename: {filename}_{sub_level_index}.json")
        compressed_data = gzip.compress(json_string.encode())
        s3_client.put_object(Body=compressed_data, Bucket=OUTPUT_BUCKET,
                             Key=f"{path}{filename}_{sub_level_index}.json.gz")


def split_json(data, filename, filetype, path, OUTPUT_BUCKET, logger, s3_client):
    """
    The function takes a JSON object, calculates the size of the JSON string in bytes, and splits the data
    into chunks based on a threshold size of approximately 900KB.

    The function identifies the list to be split based on the input filetype, and replaces the list in the
    JSON data with chunks of the list. The new, modified JSON data is then saved into a new JSON file.

    Each chunk is uploaded to an S3 bucket as a separate gzipped JSON file. The filenames are suffixed with
    an index to distinguish them from each other.

    If an exception occurs during the process, it is logged and re-raised.

    Parameters:
    - data (dict): The input JSON data to be split.
    - filename (str): The base filename to be used for the split JSON files.
    - filetype (str): The type of the file, used to identify the list to be split in the JSON data.
    - path (str): The path to the location where the split JSON files will be stored.
    - OUTPUT_BUCKET (str): The name of the S3 bucket where the split JSON files will be stored.
    - logger (logging.Logger): An instance of a logger, to log any errors during the process.
    - s3_client (boto3.client): The S3 client used to interact with AWS S3.

    Raises:
    - Exception: If an error occurs during the process, the function will log the error and re-raise the exception.
    """
    try:
        total_size_in_bytes = sys.getsizeof(json.dumps(data))

        if filetype == "SNAPSHOT":
            original_list = data['xml_data']['snapshot']['inventory_list']['inventory']
        elif filetype == "DOCUMENT":
            original_list = data['xml_data']['document_list']['document']
            # Specific cases when the document has ONLY one document
            if len(original_list) == 1:
                one_document = True
                original_list = data['xml_data']['document_list']['document'][0]['document_lines']['document_line']
                if len(original_list) == 1:
                    original_list = \
                    data['xml_data']['document_list']['document'][0]['document_lines']['document_line'][0]['line_dtls'][
                        'inventory']
                    logger.info(len(original_list))
                    one_line = True
                else:
                    one_line = False
            else:
                one_document = False
        elif filetype == "APPOINTMENT":
            original_list = data['xml_data']['appointment_list']['appointment']
        elif filetype == "TRADING_PARTNERS":
            original_list = data['xml_data']['trading_partner_list']['trading_partner']
        elif filetype == "ITEMINFO":
            original_list = data['xml_data']['iteminfo_list']['iteminfo']
        elif filetype == "MANUAL_KPI":
            original_list = data['xml_data']['manual_kpi']['kpi_list']['kpi']
        elif filetype == "ADJUSTMENT":
            original_list = data["xml_data"]["inventory_adjustment"]["in"]["inventory"]
        elif filetype == "LOCATION_MASTER":
            original_list = data['xml_data']['location_master']['location_info']

        num_items = len(original_list)
        avg_item_size = total_size_in_bytes / num_items
        logger.info(f"The AVG item size for this file is {avg_item_size}")

        chunk_size = int((500 * 1024) / avg_item_size) + 1  # 1024*1024 bytes is 1 MB

        if chunk_size <= 1:
            chunk_size = 2

        chunks = [original_list[i:i + chunk_size] for i in range(0, len(original_list), chunk_size)]
        logger.info(f"File will be splitted in ~{round(len(original_list) / chunk_size, 0)} chunks.")

        for i, chunk in enumerate(chunks):

            if filetype == "SNAPSHOT":
                data['xml_data']['snapshot']['inventory_list']['inventory'] = chunk
            elif filetype == "DOCUMENT":
                if one_document:
                    if one_line:
                        data['xml_data']['document_list']['document'][0]['document_lines']['document_line'][0][
                            'line_dtls']['inventory'] = chunk
                    else:
                        data['xml_data']['document_list']['document'][0]['document_lines']['document_line'] = chunk
                else:
                    data['xml_data']['document_list']['document'] = chunk
            elif filetype == "APPOINTMENT":
                data['xml_data']['appointment_list']['appointment'] = chunk
            elif filetype == "TRADING_PARTNERS":
                data['xml_data']['trading_partner_list']['trading_partner'] = chunk
            elif filetype == "ITEMINFO":
                data['xml_data']['iteminfo_list']['iteminfo'] = chunk
            elif filetype == "MANUAL_KPI":
                data['xml_data']['manual_kpi']['kpi_list']['kpi'] = chunk
            elif filetype == "ADJUSTMENT":
                data['xml_data']['inventory_adjustment']['in']['inventory'] = chunk
            elif filetype == "LOCATION_MASTER":
                data['xml_data']['location_master']['location_info'] = chunk

            # Determine whether it's a single chunk or not
            is_single_chunk = len(chunk) == 1 and sys.getsizeof(json.dumps(chunk)) <= 900 * 1024

            # Depending on whether it's a single chunk or not, adjust the next_filename
            next_filename = f"{filename}_{i + 1}" if is_single_chunk else f"{filename}_{i + 1}_"

            # Then use next_filename instead of the old filename
            data["filename_chunk"] = next_filename

            try:
                if 'one_line' in locals():
                    split_and_upload(data, chunk, chunk_size, f"{filename}_{i + 1}", path, OUTPUT_BUCKET, s3_client,
                                     logger, filetype, one_document=(filetype == "DOCUMENT" and one_document),
                                     one_line=(filetype == 'DOCUMENT' and one_line), level=1)
                else:
                    split_and_upload(data, chunk, chunk_size, f"{filename}_{i + 1}", path, OUTPUT_BUCKET, s3_client,
                                     logger, filetype, one_document=(filetype == "DOCUMENT" and one_document), level=1)
            except Exception as ex:
                logger.error(f"Error occurred while trying to split and upload chunk {i + 1} - {ex}")
                raise
    except Exception as ex:
        logger.exception(f"Error in split_json - {ex}")
        raise ex


def get_chunk_size(original_list, target_size_kb):
    """
    Estimate the chunk size for items.

    Parameters:
    - originallist (list): The list of items.
    - target_size_kb (int): The target size of each chunk in KB.

    Returns:
    - int: The estimated number of inventory items in each chunk.
    """
    total_size = sys.getsizeof(json.dumps(original_list))
    avg_item_size = total_size / len(original_list)
    chunk_size = int(target_size_kb * 1024 / avg_item_size)
    chunks = [original_list[i:i + max(chunk_size // 2, 1)] for i in
              range(0, len(original_list), max(chunk_size // 2, 1))]
    return chunks, chunk_size

