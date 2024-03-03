from tqdm import tqdm
from datetime import timedelta
from collections import OrderedDict
from typing import Dict
from copy import deepcopy
import argparse

from attrs import define
import pandas as pd

from utils.constants import NEW_COLUMNS, RAW_MERGED_DATA_DIR_PATH, \
    CURATED_ALL_DATA, CURATED_GENERAL_PARKING_DATA, CURATED_DBS_PARKING_DATA

# Unroll the NEW_COLUMNS
DATE, TIME, GENERAL_PARKING, DBS_PARKING = NEW_COLUMNS

def argument_parser():
    """
    Argument parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, choices=["database", "csvs"], required=True,
        help="Source of the data. If 'database' goes to the created database to look for a table wih all the data.' \
            'If 'csvs' goes to the raw_merged folder to look for the .csvs with the data")
    return parser.parse_args()

@define
class OneDayTimeSeries:
    """
    Class to store the data of one day
    Attributes:
        date: str. Date of the day
        df: pd.DataFrame. Dataframe with the data of the day
        its_empty: Dict[str, bool]. Marks for each parking if the day has data or not
        has_missing_data: Dict[str, bool]. Marks for each parking if the day has missing data
        has_meaningful_data: Dict[str, bool]. Marks for each parking if the day has meaningful data
        
    """
    date: str
    df: pd.DataFrame
    its_empty: Dict[str, bool]  # No data available for the day
    has_missing_data: Dict[str, bool]  # Day has data but some of it is missing
    has_meaningful_data: Dict[str, bool]  # Day has data that provides meaningful information

    def __init__(self, date, its_empty, df, has_meaningful_data=None):
        self.date = date
        self.its_empty = its_empty
        self.has_missing_data = None
        self.df = df
        self.has_missing_data = self.check_missing_data()
        self.check_meaningful_data_format_and_assign_to_attribute(has_meaningful_data)


    def check_missing_data(self) -> Dict[str, bool]:
        """
        Returns True if the dataframe has missing data, False otherwise
        """
        # Check if the dataframe has missing data
        has_missing_data = {
            GENERAL_PARKING: False,
            DBS_PARKING: False
        }
        for k in has_missing_data.keys():
            if self.df[k].isnull().values.any():
                has_missing_data[k] = True
        return has_missing_data
    
    def check_meaningful_data_format_and_assign_to_attribute(self, has_meaningful_data) -> Dict[str, bool]:
        """
        
        """
        if has_meaningful_data is None:
            self.has_meaningful_data = {GENERAL_PARKING: True, DBS_PARKING: True}
        else:
            assert isinstance(has_meaningful_data, dict)
            for k in has_meaningful_data.keys():
                assert k in [GENERAL_PARKING, DBS_PARKING], "Wrong key in has_meaningful_data"
            self.has_meaningful_data = has_meaningful_data

    def __repr__(self):
        return f"OneDayTimeSeries(date={self.date}, its_empty={self.its_empty},' \
                'has_missing_data={self.has_missing_data}, has_meaningful_data={self.has_meaningful_data}, df={self.df.head()})"
    

def correct_time_format(row):
    """
    Corrects the error on the time column in which some rows do not
    have the miliseconds
    """
    splited = row.split('.')
    if len(splited) > 1:
        return splited[0]    
    return splited[0]

def check_days_with_missing_data(data: OrderedDict):
    """
    Prints the days with missing data
    """
    for d in data.values():
        for parking, has_missing_data in d.has_missing_data.items():
            if has_missing_data and not d.its_empty[parking]:
                number_of_missings = len(d.df.loc[d.df[parking].isna()])
                print(f'Day {d.date} has {number_of_missings} missings for {parking}')


def get_current_day_df_with_minute_freq(df_current_day: pd.DataFrame, timerange: pd.DatetimeIndex)-> pd.DataFrame:
    """
    Returns a dataframe with the minute frequency for the current day
    """
    current_day_info = []
    
    for t in timerange[:-1]:
        # We set the expand minutes to 0, to look for the current minute only
        # for the moment
        expand_minutes = 0

        # For every minute, we get all the records and take the minimum
        current_minute = t.strftime('%H:%M:%S')
        next_minute = (t + timedelta(minutes=1)).strftime('%H:%M:%S')
        
        df_current_minute = df_current_day.between_time(current_minute, next_minute, inclusive='left')

        # In case no data is available for the current minute, we take the
        # info from other minutes
        if df_current_minute.empty:
            # We take the median of the closest minutes if we have no data for 
            # the current minute
            expand_minutes = 1
            while expand_minutes < 10:  # Expand minutes is a hyperparameter
                # We expand the minutes to the left
                prev_minute = (t - timedelta(minutes=expand_minutes)).strftime('%H:%M:%S')
                posterior_minute = (t + timedelta(minutes=expand_minutes)).strftime('%H:%M:%S')
                df_minutes_prev = df_current_day.between_time(prev_minute, next_minute, inclusive='left')
                df_minutes_next = df_current_day.between_time(current_minute, posterior_minute, inclusive='left')
                if df_minutes_prev.empty and not df_minutes_next.empty:
                    df_current_minute = df_minutes_next
                    # In case not minutes back are available, we take the next
                    # This case will only happen in the first minute of the day
                    # We take the minimum of the minutes next
                    break
                
                elif not df_minutes_prev.empty and df_minutes_next.empty:
                    # In case no minutes next are available, we take the previous
                    # This case would be the usual case when the server is down
                    # for 2 to 10 minutes throughout the day
                    df_current_minute = df_minutes_prev
                    break

                elif not df_minutes_prev.empty and not df_minutes_next.empty:
                    # In case both are available, we take the min of both
                    # This would be the usual case when the server is down
                    # only for 1 minute throughout the day
                    # TODO: Maybe this case is not necessary, so we should test
                    #   it and remove it if it is not necessary
                    df_combined = pd.concat([df_minutes_prev, df_minutes_next])
                    df_current_minute = df_combined
                    break

                # In case no minutes backward nor next are available, we continue searching
                else:
                    expand_minutes += 1
            
            # TODO: This has to be included in a log
            if expand_minutes == 15:
                pass

        if 15 > expand_minutes >= 1:
            pass

        current_day_info.append(df_current_minute.min().to_list())
    
    # We add the last minute
    current_day_info.append(current_day_info[-1])

    return current_day_info


def check_one_day_has_meaningless_data(one_day_data: OneDayTimeSeries, diff_threshold: int = 10) -> Dict[str, bool] or None:
    """
    Returns True if data is meaningless, False otherwise.
    Rules:
        #1: the difference between the minimum and maximum 
            occupation is less than diff_threshold
        #2: the difference between consecutive values is equal to 0 for 70% of the day
    """
    result = {}

    if one_day_data.its_empty[GENERAL_PARKING] and one_day_data.its_empty[DBS_PARKING]:
            return None
    
    elif one_day_data.its_empty[GENERAL_PARKING] and not one_day_data.its_empty[DBS_PARKING]:
        raise Exception('This case is supposed to be handled by the previous code')
    
    elif not one_day_data.its_empty[GENERAL_PARKING] and one_day_data.its_empty[DBS_PARKING]:
        raise Exception('This case is supposed to be handled by the previous code')        

    # When data avaialble for both parkings
    else:
        for col in one_day_data.df.columns:
    
            # Condition 1: Check if the difference between the minimum and maximum
            #   free spaces is less than diff_threshold
            min_occupation = one_day_data.df[col].min()
            max_occupation = one_day_data.df[col].max()
            diff = max_occupation - min_occupation
            if diff < diff_threshold:
                result[col] = True  # Meaningless data
            else:
                result[col] = False

            # Condition 2: Check if the difference between consecutive values is
            #   equal to 0 for 99% of the day. In such case, we consider that the
            #   data is meaningless
            number_of_datapoints = len(one_day_data.df[col])
            number_of_consecutive_equal_values = one_day_data.df[col].diff(periods=1).value_counts()[0]
            percent_of_consecutive_equal_values = number_of_consecutive_equal_values / number_of_datapoints
            #print(percent_of_consecutive_equal_values)
            if number_of_consecutive_equal_values / number_of_datapoints > 0.995:  # Value found by testing
                if diff > diff_threshold:
                    print(f'{one_day_data.date}: has {percent_of_consecutive_equal_values:.2f}% of consecutive equal values for {col}')
                result[col] = True  
    return result

def mark_days_and_parking_as_containing_meaningless_data(data: OrderedDict, diff_threshold: int = 10):
    """
    Marks the days and the parking where the data is meaningless following some rules.
    args:
        data: OrderedDict with the data
        diff_threshold: threshold to consider that the data is meaningless
    returns:
        days_with_meaningful_data: Dict with the number of days with meaningful data
    """
    days_with_meaningful_data = {
        GENERAL_PARKING: 0,
        DBS_PARKING: 0
    }
    for day in data.keys():
        
        one_day_result = check_one_day_has_meaningless_data(data[day], diff_threshold)

        if one_day_result is not None:
            for k, v in one_day_result.items():
                if v:  # If data is meaningless, we mark meaningful as False
                    data[day].has_meaningful_data[k] = False
                    # print(f'{day}: Marked as containing meaningless data for {k}')
                else:  # If data is meaningful, we mark meaningful as True
                    data[day].has_meaningful_data[k] = True
                    days_with_meaningful_data[k] += 1
                    
    return days_with_meaningful_data


def create_new_df_with_same_structure_but_no_data(df: pd.DataFrame) -> pd.DataFrame:
    # Copy structure including data types but without the data
    df_structure_only = pd.DataFrame(columns=df.columns).astype(df.dtypes)

    # If the original DataFrame has a specialized index, replicate its structure
    if isinstance(df.index, pd.MultiIndex):
        # Example for MultiIndex (adjust as needed)
        df_structure_only.index = pd.MultiIndex(levels=[[]]*len(df.index.levels), codes=[[]]*len(df.index.levels), names=df.index.names)
    elif isinstance(df.index, pd.DatetimeIndex):
        # Example for DatetimeIndex (adjust as needed)
        df_structure_only.index = pd.DatetimeIndex([])
    else:
        # For other types of indices, you may set attributes similarly
        df_structure_only.index = df.index.__class__([])
        
    return df_structure_only


def read_data_from_merged_csvs():
    # This sort is to ensure data is introduced from oldest to newest
    data_files = os.listdir(RAW_MERGED_DATA_DIR_PATH)
    data_files.sort()
    all_dfs = []
    for file in tqdm(data_files):
        all_dfs.append(pd.read_csv(RAW_MERGED_DATA_DIR_PATH / file, sep=';', index_col=0))
    df = pd.concat(all_dfs)
    return df


##################################
# ------------ MAIN ------------ #
##################################
def main(args: argparse.Namespace):
    # TODO: Create args for this script.
    # TODO: Define a way to load all the data from the .csvs directly from the merged .csvs
    #   in raw_merged folder
    # TODO: One argument for the case we want to take the data from database, we
    #   first create the all_data.csv and then we load the all_data.csv. In case we want 
    #   to take the data from the merged .csvs, we load the merged .csvs directly with other argument
    
    # Load the data
    print('\n*** Loading the data ***')
    if args.source == "database":
        df_all_data = pd.read_csv(CURATED_ALL_DATA, sep=';', index_col=0)
        print('Data loaded from database!')
    elif args.source == "csvs":
        # TODO: This option would enable the easier formatting of the data by creating directly
        #   the OneDayTimeSeries objects from the .csvs in the raw_merged folder. 
        df_all_data = read_data_from_merged_csvs()
        print('Data loaded from csvs!')
    else:
        raise ValueError("Wrong source of the data")
    print('Showing the info, head and tail of the dataframe:\n')
    print(df_all_data.info())
    print(df_all_data.head())
    print(df_all_data.tail())
    
    ### Clean and format the data ###
    print('\n*** Cleaning and formatting the data ***')
    print('Correcting the time format and indexing by date...')
    df_all_data.time = df_all_data.time.apply(correct_time_format)
    df_all_data['datetime'] = df_all_data.date + ' ' + df_all_data.time
    df_all_data.datetime = pd.to_datetime(df_all_data.datetime, format='%Y-%m-%d %H:%M:%S')
    df_all_data = df_all_data.drop([DATE, TIME], axis=1)
    df_all_data = df_all_data[['datetime', GENERAL_PARKING, DBS_PARKING]]
    df_all_data = df_all_data.set_index("datetime")
    print('Completed! Showing the head and tail of the dataframe:')
    print(df_all_data.head())
    print(df_all_data.tail())

    # Create a list with the object of time series for each day
    print('\nProperly formatting the data to enable cleaning ...')
    start_date = df_all_data.index[0]
    end_date = df_all_data.index[len(df_all_data.index)-1]
    daterange = pd.date_range(start_date.date(), end_date.date())
    all_data = OrderedDict()
    its_empty_true = {
        GENERAL_PARKING: True,
        DBS_PARKING: True
    }
    its_empty_false = {
        GENERAL_PARKING: False,
        DBS_PARKING: False
    }

    # Iterate over the days
    for d in tqdm(daterange):
        current_day_found = 0
        current_day = d.date().strftime('%Y-%m-%d')
        timerange = pd.date_range(f'{current_day} 06:30:00', f'{current_day} 20:30:00', freq='min')
        df_current_day = df_all_data.loc[current_day]
        
        if df_current_day.empty:
            all_data[current_day] = OneDayTimeSeries(
                date=current_day,
                its_empty=deepcopy(its_empty_true),  # deepcopy is necessary to avoid being all references to the same object
                df=pd.DataFrame(columns=[GENERAL_PARKING, DBS_PARKING])
            )
            
        else:
            # If found, we must get a df with a freq of minutes from 
            # 06:30:00 to 20:30:00
            current_day_data = get_current_day_df_with_minute_freq(df_current_day, timerange)
            all_data[current_day] = OneDayTimeSeries(
                date=current_day,
                its_empty=deepcopy(its_empty_false), # deepcopy is necessary to avoid being all references to the same object
                df=pd.DataFrame(data=current_day_data, index=timerange, columns=[GENERAL_PARKING, DBS_PARKING])
            )

    # Rule 1: Remove data with too many missings
    print('Rule 1: remove data with too many missings')
    check_days_with_missing_data(all_data)
    for d in all_data.values():
        for parking in d.has_missing_data.keys():
            if d.has_missing_data[parking]:
                d.df[parking] = pd.Series(name=parking)
                d.its_empty[parking] = True
                d.has_missing_data[parking] = False

    # Rule 2: Remove meaningless days
    print('Rule 2: remove meaningless days')
    print('A day is meaningless if one of two conditions is met:')
    print('\t A) The difference between the minimum and maximum occupation is less than threshold (we use 10)')
    print('\t B) The difference between consecutive values is equal to 0 for 99.5% of the day')
    days_with_meaningful_data = mark_days_and_parking_as_containing_meaningless_data(all_data)

    # Rule 3: Do not include empty days in the final .csvs
    print('Rule 3: Do not include empty days in the final .csv files')
    print('Creating the final .csv files...')
    # Create the time series dataframe for the general parking and the DBS parking
    df_general_parking = create_new_df_with_same_structure_but_no_data(all_data["2022-04-13"].df).drop(columns=[DBS_PARKING])
    df_dbs_parking = create_new_df_with_same_structure_but_no_data(all_data["2022-04-13"].df).drop(columns=[GENERAL_PARKING])
    for day in all_data.keys():

        # General parking
        empty = all_data[day].its_empty[GENERAL_PARKING]
        meaningful = all_data[day].has_meaningful_data[GENERAL_PARKING]
        missing_data = all_data[day].has_missing_data[GENERAL_PARKING]
        if meaningful and not empty and not missing_data:
            df_general_parking = pd.concat([df_general_parking, pd.DataFrame(all_data[day].df[GENERAL_PARKING])])    

        # DBS parking
        empty = all_data[day].its_empty[DBS_PARKING]
        meaningful = all_data[day].has_meaningful_data[DBS_PARKING]
        missing_data = all_data[day].has_missing_data[DBS_PARKING]
        if meaningful and not empty and not missing_data:
            df_dbs_parking = pd.concat([df_dbs_parking, pd.DataFrame(all_data[day].df[DBS_PARKING])])
    
    # Save curated data to csv
    df_general_parking.to_csv(CURATED_GENERAL_PARKING_DATA, sep=';')
    df_dbs_parking.to_csv(CURATED_DBS_PARKING_DATA, sep=';')

    print(f'Data curated and saved to {CURATED_GENERAL_PARKING_DATA.name} and {CURATED_DBS_PARKING_DATA.name}')
    print('Process finished!')


if __name__ == "__main__":
    main(args=argument_parser())