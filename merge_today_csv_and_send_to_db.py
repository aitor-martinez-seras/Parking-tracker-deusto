from datetime import datetime
import logging

import pandas as pd
import pytz

import db
from utils.constants import UNMERGED_DATA_DIR_PATH, RAW_MERGED_DATA_DIR_PATH, TABLE_NAME, SCHEMA_NAME, LOGS_PATH, NEW_COLUMNS
from utils.time_series import check_monotonic_increasing_dates


def main():
    # File names pattern: dataframe_YYYY_MM_DD_XX.csv where XX is a number from
    # 00 to 99 representing the order of the dataframes in that day

    today = datetime.now(pytz.timezone('Europe/Madrid'))
    searched_string = f'dataframe_{today.year}_{str(today.month).zfill(2)}_{str(today.day).zfill(2)}'

    # Define logger basic configuration
    logging.basicConfig(filename= (LOGS_PATH /f'{str(today).replace("-", "_")}_sending_raw_merged_csv.log'), filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    RAW_MERGED_DATA_DIR_PATH.mkdir(exist_ok=True)
    initial_time_for_monotonic_increase = datetime.strptime('06:00:00', '%H:%M:%S')

    retrieved_csvs = []
    # retreived_csvs_order = []
    for i, fpath in enumerate(sorted(UNMERGED_DATA_DIR_PATH.iterdir())):
        if  fpath.name.find(searched_string) >= 0:
            df = pd.DataFrame(columns=NEW_COLUMNS)
            df_new = pd.read_csv(fpath, sep=';', index_col=0)
            df_new.columns = NEW_COLUMNS
            retrieved_csvs.append(df_new)
            # retreived_csvs_order.append(int(fpath.name.split('_')[4].split('.')[0]))
            logging.info(f'Found .csv of day {today} -> {fpath.name}')

    if len(retrieved_csvs) == 0:
        logging.error(f'No dataframes for the today ({today}) where found, exiting program')
        return

    try:
        # First is very important to correctly order the csvs
        # retrieved_csvs = [x for idx, x in sorted(zip(retreived_csvs_order, retrieved_csvs))]

        # Concatenate all daily csvs and check if they increase monotonically
        df = pd.concat(retrieved_csvs, ignore_index=True)

        monotonic_increase = check_monotonic_increasing_dates(df['time'], t_initial=initial_time_for_monotonic_increase)

        if monotonic_increase:
            new_file_path = RAW_MERGED_DATA_DIR_PATH / f'{searched_string}.csv'
            df.to_csv(new_file_path, sep=';')
            logging.info(f'Succesfully saved {new_file_path} with no errors!')

        else:
            logging.warning(f'Saved csv ({new_file_path.name}) has NOT monotonically increasing dates')

    except ValueError as e:
        logging.error(e)
        raise Exception(f'No dataframes for the today ({today}) where found, exiting program')
    
    # Send to DB
    print(f'Sending merged {new_file_path} to DB...')
    df.to_sql(name=TABLE_NAME, con=db.engine, schema=SCHEMA_NAME , index=False, if_exists='append')
    print(f'File {new_file_path} succesfully uploaded to the database {db.DATABASE}, in the table {TABLE_NAME} in the schema {SCHEMA_NAME}')

if __name__ == "__main__":
    main()