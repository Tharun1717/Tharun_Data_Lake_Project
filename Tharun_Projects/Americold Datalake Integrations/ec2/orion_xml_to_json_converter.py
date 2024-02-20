'''
================================================================================================================================
SCRIPT: orion_xml_json_converter_all.py - PARSER SCRIPT
AUTHOR: Lautaro Cisterna | Data Engineer | The Hackett Group Inc. | lautaro.cisterna@thehackettgroup.com
================================================================================================================================


==================================
      Process description
==================================
- The objective of this script is to process XML files into JSON and dump them in the silver layer in s3.
In this script we also add the parsing_process function, which will be applied to the XML files in parallel using Multithreading.

==================================
        Parsing function
==================================
We created this function in this script so we can have a sense of what it does and it will be more clear if it is here than in the utilities.py file.
The whole process of parsing is within this function.

- Steps:
    1. Grab file from s3 and parse it.
    2. Obtain output path to dump json in silver layer.
    3. Dump json in s3.
        - If the file contains any problem and cannot be parsed, an email will be sent using SNS.
        - For files that weights more than 1 MB, these files will be splitted in chunks so every chunk weights less than 1 MB and it can be copied in redshift.
    4. Move the file to the corresponding folder.
        Scenarios:
        - DESIRED: If the file was succesfully parsed, it will be moved from Processed folder to Parsed folder in the bronze layer.
        - UNKNOWN FILETYPE: If the filetype is unknown, it will be moved from Processed folder to Unknown_filetype in the bronze layer.
        - ERROR: If the XML file is empty or the filename is not valid or the processer encountered an error during parsing process, the file
         will be moved from Processed folder to Parsing_error folder in the bronze layer.

==================================
        MAIN Process
==================================
This will describe the main steps of the entire process or parsing.

Steps:
1. Obtain list of XML files with the desired chunksize.
    - A list of files will be obtained from s3 (the chunksize usually will be 10K - in case there are less files in the bronze layer, all of the files will be grabbed).
    This list is the list of files that are going to be parsed in the corresponding run.
    - IMPORTANT: Additionally to those 10k files, 5 SNAPSHOT files (if available) will be added to that batch. Those 10k files correspond to any filetype except SNAPSHOT.
    In summary, every time this script runs, it will grab 10k files from any filetype (except SNAPSHOT) + 5 SNAPSHOT files.
    This process was developed this way, because if we add more SNAPSHOT files in each run, the VM can run out of memory and we want to avoid that.

2. Parse all the files in parallel using Multithreading.
    - Files are going to be parsed using the Parsing function/process explained above. That specific process will happen to each XML file in parallel.

3. Sanity check to confirm that all files have been processed by the Threadpool.
    - In case all files have been processed, nothing happens and the process continues.
    - In case some files have not been processed because the threadpool didn't grab them, a function will be called so those files are parsed as well.
        - This scenario is highly unlikely, but we need to cover it just in case.

4. Send notification email - IF NECESSARY.
    - In case some files weren't parsed, an email will be sent notifying this issue.

IMPORTANT NOTES:
- This process will be run using a bash script scheduled to run all the time (24/7). So when one run is finished, after a minute another run will start.
- In case you need more information about the functions, please check utilities.py and there we have documented all the functions.
- If you have any other doubts, contact Hackett Tech Support team.
'''

# ================================================ #
#                   LIBRARIES                      #
# ================================================ #

import boto3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import json
import gzip

t0 = datetime.now()

# ================================================ #
#                   VARIABLES                      #
# ================================================ #

from src.constants import (INPUT_BUCKET,
                           OUTPUT_BUCKET,
                           FILTER_FOLDER,
                           LOCAL_LOG_PATH,
                           XML_LOG_BUCKET,
                           CHUNKSIZE,
                           MAX_WORKERS_THREADPOOL,
                           MOVE_FILES,
                           REGION)

from src.utilities import (set_logger, load_xml_list,
                           move_xml_files, email_notification,
                           convert_xml_to_json,
                           check_json_size,
                           split_json,
                           obtain_file_information,
                           process_dict
                           )

# ================================================ #
#                   LOGGER                         #
# ================================================ #

# Logger file
logger = set_logger("parser_ETL_logs", LOCAL_LOG_PATH + "parser_logs_all.txt")

# ================================================ #
#              s3 boto parameters                  #
# ================================================ #
# S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3_bucket = s3_resource.Bucket(INPUT_BUCKET)
s3_bucket_output = s3_resource.Bucket(OUTPUT_BUCKET)

# SNS Client
sns = boto3.client('sns', region_name=REGION)

# ================================================ #
#              Sanity check variables              #
# ================================================ #
move_files = MOVE_FILES

# Sanity check lists
problem_files = {}
parsed_files = []
unknown_filetypes = []


# ================================================ #
#              parsing_process function            #
# ================================================ #
def parsing_process(file):
    """
    The function takes a file as input and carries out the following steps:

    1. It extracts information from the file path, like facility_id, WMS (Warehouse Management System),
       filename, parsing_date, filetype, and a new path structure.
    2. If the file type is classified as 'UNKNOWN', it moves these files to an 'unknown_filetype' directory.
    3. If the file type is not 'UNKNOWN', it converts the XML data in the file to JSON.
    4. It processes the JSON data according to the file type.
    5. It constructs a new JSON object containing the file information and the processed JSON data.
    6. It checks the size of the final JSON data. If it's more than 1 MB, the data is split into smaller chunks.
    7. The final JSON data (either split or original) is saved to an OUTPUT_BUCKET in a .gz format.
    8. If enabled, the parsed XML files are moved to a 'parsed' directory.

    Parameters:
    - file (str): The file path of the XML file to be parsed and processed.

    Raises:
    - Exception: If any error occurs during the parsing process, it logs the error and re-raises the exception.
    """

    try:

        logger.info(f"Parsing xml - {file}")
        # Obtain data from file
        facility_id, wms, filename, parsing_date, filetype, path = obtain_file_information(file, logger)

        # Check if filetype is UNKNOWN or not used
        if filetype == "UNKNOWN":
            # Move useless files
            if move_files == True:
                try:
                    move_xml_files(s3_resource, INPUT_BUCKET, file, "unknown_filetype", logger)
                except Exception as e:
                    raise e
        else:
            # If the filetype is known, we will process it
            try:
                # Try using default decoder
                json_data = convert_xml_to_json(file, logger, s3_client, INPUT_BUCKET)
            except:
                logger.info("This file contained a non-ascii character so the parser will try a different decoder.")
                # In case it fails, we will use 'windos-1252' decoder'
                json_data = convert_xml_to_json(file, logger, s3_client, INPUT_BUCKET, decoder=True)
                pass

            # Process json_data
            process_dict(json_data, filetype, logger)

            # Create json with data from the information of the file
            if filetype == "DOCUMENT":
                final_json = {
                    "facility_id": facility_id,
                    "source_system_code": wms,
                    "filename": filename,
                    "filename_chunk": None,
                    "process_status": "0",
                    "parsing_date": parsing_date,
                    "xml_data": json_data

                }

            else:

                final_json = {
                    "facility_id": facility_id,
                    "source_system_code": wms,
                    "filename": filename,
                    "filename_chunk": None,
                    "parsing_date": parsing_date,
                    "xml_data": json_data

                }

            # Check if json file weights more than 1 MB
            split = check_json_size(json_data, logger)

            if split == True:
                # If it weights more than 1 MB, file will be splitted
                logger.info(f"{filename} JSON data weights more than 1 MB.")
                logger.info(f"Splitting {filename} in smaller chunks")
                split_json(final_json, filename, filetype, path, OUTPUT_BUCKET, logger, s3_client)
            else:
                # If it weights less than 1 MB, file will be dumped directly
                logger.info(f"{filename} JSON data weights less than 1MB.")
                json_final = json.dumps(final_json)
                compressed_data = gzip.compress(json_final.encode())
                s3_client.put_object(Body=compressed_data, Bucket=OUTPUT_BUCKET, Key=f"{path}{filename}.json.gz")

            # Append files to parsed_files list
            parsed_files.append(file)

            # Move parsed files
            if move_files == True:
                try:
                    move_xml_files(s3_resource, INPUT_BUCKET, file, "parsed", logger)

                except Exception as e:
                    raise e

    except Exception as ex:
        problem_files[file] = str(ex)
        logger.exception(f"The file {file} had an error while parsing - {ex}")


# ================================================ #
#              MAIN FUNCTION                       #
# ================================================ #
def main():
    """
    The main function that drives the whole parsing process. It carries out the following steps:

    1. It obtains a list of XML files to be parsed.
    2. Using ThreadPoolExecutor, it concurrently executes the parsing process on each file.
    3. It checks if all the files have been processed by the ThreadPoolExecutor.
    4. If there are files that have encountered errors during the parsing process (problem_files),
       it sends an email notification and moves these files to a "Parsing_error" folder.
    5. If there are files containing file types that are unknown, it sends an email notification
       and moves these files to an "Unknown_filetype" folder.
    6. It logs the number of files that have been parsed, the number of files with unknown filetypes,
       and the number of files that could not be parsed due to issues.

    Note: This function does not return anything but rather, it logs information and moves files based on their parsing outcome.
    """

    # Step 1: Reading xml file list
    logger.info("Obtaining list of XML files.")
    files = load_xml_list(s3_bucket, logger, CHUNKSIZE, "all", FILTER_FOLDER)
    logger.info(f"We are parsing {len(files)} files.")

    # Parsing files with multithreading
    logger.info("Parsing XML files using ThreadPoolExecutor")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_THREADPOOL) as executor:
        _ = executor.map(parsing_process, files)

    # Check if the threadpool missed any files:
    logger.info("Checking missing files from threadpool")
    no_problem_files = [file for file in files if file not in problem_files]
    if len(parsed_files) + len(unknown_filetypes) == len(no_problem_files):
        logger.info("All files were processed by the threadpool.")
    else:
        logger.info(f"The multithreading lost some files. These files will be parsed in the next run.")

    # Send email notification email in case we encounter some files that we could not parse.
    if len(problem_files) > 0:
        logger.info("Sending email notification for problem files that were not parsed.")
        logger.info(f"This files had problems: {problem_files}")
        email_notification(sns, problem_files, unknown_filetypes, 'problem', logger)
        logger.info("Moving files with problems to Parsing_error folder in the bronze layer bucket.")
        for problem_file in problem_files:
            if move_files == True:
                try:
                    move_xml_files(s3_resource, INPUT_BUCKET, problem_file, "problem", logger)
                except Exception as e:
                    print(e)

    else:
        logger.info("No files had problems.")

    # Send email notification email in case we encounter files related to filetypes that are unknown for us yet.
    if len(unknown_filetypes) > 0:
        logger.info("Sending email notification for unknown filetypes files that were not parsed.")
        logger.info(f"This files contained unknown filetypes: {unknown_filetypes}")
        # email_notification(sns, problem_files, unknown_filetypes, 'unknown_filetype', logger)
        logger.info("Moving files with problems to Unknown_filetype folder in the bronze layer bucket.")

        # Finishing process and summary
    logger.info("Finishing process...")
    logger.info(f"The job parsed {len(parsed_files)} files.")
    logger.info(f"The job have found {len(unknown_filetypes)} files that contained a filetype that is unknown.")
    logger.info(f"The job have found {len(problem_files)} files that had problems and could not be parsed.")


if __name__ == "__main__":
    main()

tf = datetime.now()
print(f"Total time {tf - t0}")