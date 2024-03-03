import os
from pathlib import Path


#### DF constants ####

NEW_COLUMNS = ['date', 'time', 'general_parking', 'dbs_parking']

#### Common use constants ####

WEEKDAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

#### PATHS ####

# The working directory MUST ALWAYS be the parent of utils
WORKING_DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent

# Config
CONFIG_PATH = WORKING_DIR_PATH / 'config'
DOTENV_PATH = CONFIG_PATH / '.env'

# Data
DATA_DIR_PATH = WORKING_DIR_PATH / 'Data'
UNMERGED_DATA_DIR_PATH = DATA_DIR_PATH / 'raw_not_merged'
RAW_MERGED_DATA_DIR_PATH = DATA_DIR_PATH / 'raw_merged'
CURATED_DATA_DIR_PATH = DATA_DIR_PATH / 'curated'
CURATED_ALL_DATA = CURATED_DATA_DIR_PATH / 'all_data.csv'
CURATED_GENERAL_PARKING_DATA = CURATED_DATA_DIR_PATH / 'general_parking.csv'
CURATED_DBS_PARKING_DATA = CURATED_DATA_DIR_PATH / 'dbs_parking.csv'

# Docker
DOCKER_DOTENV_PATH = WORKING_DIR_PATH / 'docker/.env'

# DB
TABLE_NAME = 'raw'
SCHEMA_NAME = 'public' 

# Logs
LOGS_PATH = WORKING_DIR_PATH / 'logs'

# Influx DB 
TOKEN = "xDv026WEeftbSYMvNmjQP-R7nFuJ4hAAY1hxgfMHYOulsJ8Ho6WtG-Tu4ykJD2Z2EFRUtrIBveKOxSkVHFCqCQ=="
ORG = "ams-home"
BUCKET = "parking-deusto"
DELAY = 15  # The delay between data points, in seconds