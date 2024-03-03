import os
from datetime import datetime, timedelta

import pandas as pd

from utils.constants import UNMERGED_DATA_DIR_PATH, RAW_MERGED_DATA_DIR_PATH, NEW_COLUMNS
from utils.time_series import check_monotonic_increasing_dates, check_all_rows_are_from_same_date


# File names pattern: dataframe_YYYY_MM_DD_XX.csv where XX is a number from
# 00 to 99 representing the order of the dataframes in that day 
def main():
    RAW_MERGED_DATA_DIR_PATH.mkdir(exist_ok=True)
    data_files = list(sorted(UNMERGED_DATA_DIR_PATH.iterdir()))
    same_day_files = 1

    separator = '_'

    initial_time_for_monotonic_increase = datetime.strptime('06:00:00', '%H:%M:%S')
    not_merged_dates = []
    for i, fname in enumerate(data_files):
        
        same_day_files -= 1  # The first time a file is found, same_day_files is 0 (starts at 1)

        # Only check for new files if there are no same day files left
        if same_day_files == 0:
            
            same_day_files = 1
            fname_split = fname.name.split('_')
            current_date = fname_split[1:4]  # Does not give error if exceeds list size
            current_date_str = '/'.join(current_date)
            
            # In case the lenght is not 5, it means it is not the file we are looking for 
            # (it is another type of file), so skip
            if len(fname_split) != 5:
                print(f'Skipped file {fname}')
                continue

            # Search for files retrieved in the same day
            for next_fname in data_files[i+1:]:
                next_date = next_fname.name.split('_')[1:4]
                if next_date == current_date:
                    same_day_files += 1
                else:
                    # As the files are sorted, if the next file is not from the same day,
                    # the rest won't be either so we can break the loop an save some time
                    break
            
            # Now we got all the same day files in a list
            same_day_file_paths = data_files[i:i+same_day_files]
            
            # Concat all the csvs of the same day in one dataframe
            df = pd.DataFrame(columns=NEW_COLUMNS)
            for csv_file in same_day_file_paths:
                df_new = pd.read_csv(csv_file, sep=';', index_col=0)
                df_new.columns = NEW_COLUMNS
                df = pd.concat([df, df_new], ignore_index=True)

            # Checks
            print(f'--- Checking {current_date_str} ---')
            monotonic_increase = check_monotonic_increasing_dates(
                df['time'],
                t_initial=initial_time_for_monotonic_increase
            )

            all_rows_from_current_date = check_all_rows_are_from_same_date(
                df['date'],
                searched_date=f'{current_date[0]}-{current_date[1]}-{current_date[2]}'
            )

            # Final save
            if monotonic_increase and all_rows_from_current_date:
                print(f'All checks passed, saving dataframe to csv...')
                if fname.name.split('_')[0] == 'dataframe':
                    new_name = fname.name.split('_')[:4]
                    new_name = separator.join(new_name) + '.csv'
                    df.to_csv(RAW_MERGED_DATA_DIR_PATH / new_name, sep=';')
                    print(f'Succesfully saved {new_name}\n')
                else:
                    print(f'File with name {fname.name} not saved, as does not comply to the pattern\n')
            elif not monotonic_increase and not all_rows_from_current_date:
                print(f'Found a non monotonic increase in the timestamp!')
                print(f'Not all rows of the csv are from the same date!')
                print(f'Data from {current_date} not saved, as data does not increase monotonically and not all rows are from the same date\n')
            elif not monotonic_increase:
                print(f'Found a non monotonic increase in the timestamp!')
                print(f'Data from {current_date} not saved, as data does not increase monotonically\n')
            elif not all_rows_from_current_date:
                print(f'Not all rows of the csv are from the same date!')
                print(f'Data from {current_date} not saved, as not all rows are from the same date\n')
            else:
                raise AssertionError('This should not happen')
            
            if not monotonic_increase or not all_rows_from_current_date:
                not_merged_dates.append(current_date_str)

        else:
            continue


if __name__ == "__main__":
    main()
