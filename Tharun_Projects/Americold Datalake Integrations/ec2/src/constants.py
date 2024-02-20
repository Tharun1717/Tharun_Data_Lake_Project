
import os

# ====================================================
#              Obtaining enviroment
# ====================================================
EC2_ENV = os.environ.get("EC2_ENV", "UNKNOWN")


# ====================================================
#              Logger path
# ====================================================
LOCAL_LOG_PATH = "/home/ubuntu/Americold.i3pl.DataLake.Integrations/ec2/xmlparser/logs/"

# ====================================================
#              EMAIL NOTIFICATION
# ====================================================
REGION = 'us-west-2'

# ====================================================
#              S3 BUCKET AND EMAIL PARAMETERS
# ====================================================
if EC2_ENV == "DEV":
    print("We are in a DEV enviroment.")
    INPUT_BUCKET = 'i3pl-xml-bronze-dev'
    OUTPUT_BUCKET = 'i3pl-json-silver'
    TOPIC_ARN = 'arn:aws:sns:us-west-2:646632670602:testing_topic_jetparser'
elif EC2_ENV == "UAT":
    print("We are in a UAT enviroment.")
    INPUT_BUCKET = 'i3pl-xml-bronze-uat'
    OUTPUT_BUCKET = 'i3pl-json-silver-uat'
    TOPIC_ARN = 'arn:aws:sns:us-west-2:598936376082:JetParserNotifications'
elif EC2_ENV == "PROD":
    print("We are in a PROD enviroment.")
    INPUT_BUCKET = 'i3pl-xml-bronze-prd'
    OUTPUT_BUCKET = 'i3pl-json-silver-prd'
    TOPIC_ARN = 'arn:aws:sns:us-west-2:646632670602:testing_topic_jetparser' # Needs change

S3_INPUT_PATH = 's3a://' + INPUT_BUCKET
S3_OUTPUT_PATH = 's3a://' + OUTPUT_BUCKET
FILTER_FOLDER = "Processed/"
#FILTER_FOLDER = "Parsing_error/"
XML_LOG_BUCKET = "XML_FILES_AUDIT_LOG/"


# ====================================================
#              CODE PARAMETERS
# ====================================================
CHUNKSIZE = 10000
MAX_WORKERS_THREADPOOL = 16
MAX_MISSING_FILES = 50
MOVE_FILES = True

KEYS_DICT = {
            'ITEMINFO': ['iteminfo'],
            'APPOINTMENT': ['appointment', 'note'],
            'TRADING_PARTNERS': ['trading_partner'],
            'SNAPSHOT': ['inventory'],
            'DOCUMENT': ['document', 'document_line', 'inventory', 'document_note', 'pallet'],
            'MANUAL_KPI': ['kpi', 'customer_details', 'pallet_occupancy_details'],
            'ADJUSTMENT': ['inventory'],
            'LOCATIONMASTER': ['location_info']
                }

# ====================================================
#              DATE FORMAT AND MOTN DICT
# ====================================================
MSG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MONTH_DICT = {'January':'01',
		'February':'02',
		'March':'03',
		'April':'04',
		'May':'05',
		'June':'06',
		'July':'07',
		'August':'08',
		'September':'09',
		'October':'10',
		'November':'11',
		'December':'12'	}


