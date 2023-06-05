import os
from pathlib import Path

# The working directory must always be the parent of utils
WORKING_DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent

# Data
DATA_DIR_PATH = WORKING_DIR_PATH / 'Data'
UNMERGED_DATA_DIR_PATH = DATA_DIR_PATH / 'raw_not_merged'
RAW_MERGED_DATA_DIR_PATH = DATA_DIR_PATH / 'raw_merged'

# Docker
DOTENV_PATH = WORKING_DIR_PATH / 'docker/.env'

# DB
TABLE_NAME = 'raw'

# Logs
LOGS_PATH = WORKING_DIR_PATH / 'logs'

# Influx DB 
TOKEN = "xDv026WEeftbSYMvNmjQP-R7nFuJ4hAAY1hxgfMHYOulsJ8Ho6WtG-Tu4ykJD2Z2EFRUtrIBveKOxSkVHFCqCQ=="
ORG = "ams-home"
BUCKET = "parking-deusto"
DELAY = 15  # The delay between data points, in seconds