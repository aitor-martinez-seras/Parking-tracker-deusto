from pathlib import Path

# Influx DB information
TOKEN = "xDv026WEeftbSYMvNmjQP-R7nFuJ4hAAY1hxgfMHYOulsJ8Ho6WtG-Tu4ykJD2Z2EFRUtrIBveKOxSkVHFCqCQ=="
ORG = "ams-home"
BUCKET = "parking-deusto"
DELAY = 15  # The delay between data points, in seconds

DATA_DIR_PATH = Path('Data')
UNMERGED_DATA_DIR_PATH = DATA_DIR_PATH / 'raw_not_merged'
RAW_MERGED_DATA_DIR_PATH = DATA_DIR_PATH / 'raw_merged'