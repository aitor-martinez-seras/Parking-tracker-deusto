import os

import pandas as pd

from utils.constants import UNMERGED_DATA_DIR_PATH, RAW_MERGED_DATA_DIR_PATH

# File names pattern: dataframe_YYYY_MM_DD_XX.csv where XX is a number from
# 00 to 99 representing the order of the dataframes in that day 

new_columns = ['date', 'time', 'general_parking', 'dbs_parking']
RAW_MERGED_DATA_DIR_PATH.mkdir(exist_ok=True)

data_files = os.listdir(UNMERGED_DATA_DIR_PATH)
same_day_files = 1

separator = '_'

for i, fname in enumerate(sorted(UNMERGED_DATA_DIR_PATH.iterdir())):
    
    same_day_files -= 1

    # Only check for new files if there are no same day files left
    if same_day_files == 0:
        
        same_day_files = 1
        fname_split = fname.name.split('_')
        date = fname_split[1:4]  # Does not give error if exceeds list size
        
        # In case the lenght is not 5, it means it is not the file 
        # we are looking for, so skip
        if len(fname_split) != 5:
            print(f'Skipped file {fname}')
            continue

        # Search for files retrieved in the same day
        for next_fname in data_files[i+1:]:
            next_date = next_fname.split('_')[1:4]
            if next_date == date:
                same_day_files += 1
            else:
                break

        same_day_file_names = data_files[i:i+same_day_files]

        df = pd.DataFrame(columns=new_columns)
        for csv_file in same_day_file_names:
            df_new = pd.read_csv(UNMERGED_DATA_DIR_PATH / csv_file, sep=';', index_col=0)
            df_new.columns = new_columns
            df = pd.concat([df, df_new], ignore_index=True)
        
        if fname.name.split('_')[0] == 'dataframe':
            new_name = fname.name.split('_')[:4]
            new_name = separator.join(new_name) + '.csv'
            df.to_csv(RAW_MERGED_DATA_DIR_PATH / new_name, sep=';')

            print(f'Succesfully saved {new_name}')
        else:
            print(f'File with name {fname.name} not saved, as does not comply to the pattern')

    else:
        continue