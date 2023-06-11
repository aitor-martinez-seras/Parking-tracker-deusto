from datetime import date

import pandas as pd

import db
from utils.constants import UNMERGED_DATA_DIR_PATH, RAW_MERGED_DATA_DIR_PATH, TABLE_NAME, SCHEMA_NAME


def main():
    # File names pattern: dataframe_YYYY_MM_DD_XX.csv where XX is a number from
    # 00 to 99 representing the order of the dataframes in that day 

    new_columns = ['date', 'time', 'general_parking', 'dbs_parking']
    RAW_MERGED_DATA_DIR_PATH.mkdir(exist_ok=True)

    today = date.today()
    searched_string = f'dataframe_{today.year}_{str(today.month).zfill(2)}_{str(today.day).zfill(2)}'

    retrieved_csvs = []
    for i, fpath in enumerate(UNMERGED_DATA_DIR_PATH.iterdir()):
        if  fpath.name.find(searched_string) >= 0:
            df = pd.DataFrame(columns=new_columns)
            df_new = pd.read_csv(fpath, sep=';', index_col=0)
            df_new.columns = new_columns
            retrieved_csvs.append(df_new)
            print(f'Found .csv of day {today} -> {fpath.name}')

    if len(retrieved_csvs) == 0:
        print(f'No dataframes for the today ({today}) where found, exiting program')

    try:
        df = pd.concat(retrieved_csvs, ignore_index=True)
        new_file_path = RAW_MERGED_DATA_DIR_PATH / f'{searched_string}.csv'
        df.to_csv(new_file_path, sep=';')
        print(f'Succesfully saved {new_file_path}')
    except ValueError as e:
        print(e)
        raise Exception(f'No dataframes for the today ({today}) where found, exiting program')
    
    # Send to DB
    print(f'Sending merged {new_file_path} to DB...')
    df.to_sql(name=TABLE_NAME, con=db.engine, schema=SCHEMA_NAME , index=False, if_exists='append')
    print(f'File {new_file_path} succesfully uploaded to the database {db.DATABASE}, in the table {TABLE_NAME} in the schema {SCHEMA_NAME}')

if __name__ == "__main__":
    main()