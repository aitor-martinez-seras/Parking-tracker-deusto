import os
import sys
from pathlib import Path
import sched, time
from datetime import datetime
import logging
import signal

import requests
import pytz
from bs4 import BeautifulSoup
from pandas import DataFrame

from utils.constants import UNMERGED_DATA_DIR_PATH, LOGS_PATH


def get_general_DBS_parking_free_slots(df_parking_data: DataFrame, file_name: str):
    """
    Function that scrapes the webpage of the parking, collects the parking occupation and inserts a row with the info in
    the dataframe.
    :param df_parking_data: Dataframe wehre data is going to be stored
    :param file_name: str containing the file name
    """
    # Scrape the page where info is available
    page = requests.get('http://parking.deusto.es/')
    now = datetime.now(pytz.timezone('Europe/Madrid'))
    logging.info(now.time())

    #Get HTML content
    soup = BeautifulSoup(page.text, 'html.parser')
    n = 0

    # Initialize the string variables where the number of spaces available in the General parking and the DBS parking
    # are going to be stored
    general_number = ''
    dbs_number = ''

    # Following code is for navigating the HTML
    for img_tag_name in soup.find_all('img'):
        if 'images/number' in img_tag_name['src'] and n<3:
          general_number = general_number + img_tag_name['src'][13]
          n += 1
        elif 'images/number' in img_tag_name['src'] and n>=3:
          dbs_number = dbs_number + img_tag_name['src'][13]

    # Cast to integers
    # TODO: Error preventing of possible fails when general number and dbs number are not scrapped corretly, we need to
    #   know if the error comes from bad scrapping
    general_number = int(general_number)
    dbs_number = int(dbs_number)

    # Append elements to the Dataframe
    df_parking_data.loc[df_parking_data['Time'].size] = {
        'Date': now.date(),
        'Time': now.time(),
        'General': general_number,
        'DBS': dbs_number
    }

    # Enter again the action in the scheduler
    s.enter(delay=DELAY, priority=1, action=get_general_DBS_parking_free_slots, argument=(df_parking_data,file_name))


def complete_file_name(file_name: str, data_dir_path: Path) -> str:
    """
    Returns the complete file name (of the form dataframe_(year)_(month)_(day)_(number of files within one day))
    by searching trought the directory for files of the same day
    :param file_name: str containing the file name
    :param data_dir_name:  str containing the name of the directory where data is going to be stored
    :return: str with the complete file name
    """
    list_existing_files = os.listdir(data_dir_path)
    coincidences = []
    # Check in the directory if there is a file of the same day already
    for item in list_existing_files:
        # Search among files in the directory, the ones that are of the current day by matching the file_name, that for
        # the moment does not have the last two numbers that refer to the number of the file within one day
        string_found = int(item.rfind(file_name + '_')) # 0 if found, -1 if not found
        if string_found == 0:
            # We append the last number of the file that refers to the number of files in the specified day (specified)
            # by the previous numbers in the name. We split the string by the "_", take the last position of the
            # generated list and then extract the two numbers
            coincidences.append(int(item.split("_")[-1][:2]))

    # If there are coincidences, we sum 1 to the number not overwrite the .csv of that day
    if len(coincidences) > 0:
        # We extract the greatest number and sum 1 to generate the next .csv of the day
        number_of_csv_today = str(max(coincidences)+1)
        file_name = file_name + '_' + number_of_csv_today.zfill(2) + '.csv'
        logging.info(f'Number of .csv today is {number_of_csv_today}')
    elif len(coincidences) == 0:
        # If coincidences = [], that means either that there are not files in the directory or that the
        # files in the directory are of other days, therefore we put 00 as the number
        file_name = f'{file_name}_00.csv'
    return file_name


def save_csv(parking_data: DataFrame, file_name: str, data_dir_path: Path, backup=False):
    """
    Saves the dataframe to the .csv file with the name file_name in the directory data_dir_name
    :param parking_data: Dataframe with the data
    :param file_name: str with the file name
    :param data_dir_name:  str containing the name of the directory where data is going to be stored
    """
    # Save to a .csv the data generated
    if backup is False:
        # When not saving a backup, we need to complete the file name with the number of .csv of the day
        file_name = complete_file_name(file_name, data_dir_path)
    # Create the file_path by appending the variables to the Path object with the "/" operator
    file_path = data_dir_path / file_name
    parking_data.to_csv(file_path, sep=';', decimal=',')
    logging.info(f'{file_name} saved in {file_path}!')


def main():
    # SIGTERM signal handler
    def sigterm_handler(signal, frame):
        """
        When receiving the SIGTERM signal from the crontab job, save the .csv files
        """
        logging.info('Python script exiting gracesfully after catching the SIGTERM signal')
        # The sys.exit(0) raises SystemExit exception, that is catched by the try/except clauses that are
        # responsible for saving the .csv, therefore there is no need to save the .csv here
        sys.exit(0)
    # Register the handler
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Make scheduler and the DELAY constants global
    global s, DELAY
    # Define the delay (in seconds)
    DELAY = 15

    # Constant definition
    today = datetime.now(pytz.timezone('Europe/Madrid'))

    # Define logger basic configuration
    logging.basicConfig(filename= (LOGS_PATH / f'{str(today.date()).replace("-", "_")}_app.log'), filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Create folder where data will be stored 
    UNMERGED_DATA_DIR_PATH.mkdir(parents=True, exist_ok=True)

    # The file names will be dataframe followed by the year, month, day and a number between 00 and 99. The last number
    # is for the case where exceptions happen during exectuions and more than one dataframe per day is needed
    # File names -> dataframe_(year)_(month)_(day)_(number of files in current day)
    file_name = 'dataframe' + '_' + str(today.year) + '_' + str(today.month).zfill(2) + '_' + str(today.day).zfill(2)
    df_parking_data = DataFrame(columns=['Date', 'Time', 'General', 'DBS'])

    # Here we define the time function and the sleep function for the scheduler
    s = sched.scheduler(time.time, time.sleep)
    # Queue the event of retrieving the data
    s.enter(delay=DELAY, priority=1, action=get_general_DBS_parking_free_slots, argument=(df_parking_data, file_name))
    
    # MAIN LOOP
    # Inside the get_general_DBS_parking_free_slots() function another event is queued, therefore when using s.run()
    # we enter an infinite loop that ends when Ctrl+C or the SIGTERM is received.
    # We handle every exception to check if we are killing the program or some other exception happened, so in that case
    # we can call the program to restart itself
    try:
        s.run() # Makes the scheduler start
    except BaseException as e:
        # In case we have an exception of any kind, we want to save the retrieved data to a .csv file if the dataframe
        # is not going to be empty
        if len(df_parking_data.index) > 9: # save .csv only if we have meaningfull data
            save_csv(df_parking_data, file_name, UNMERGED_DATA_DIR_PATH)
        # If the exception is not a KeyBoardInterrup nor a SytemExit (they do not inherit from Exception),
        # program must continue and therefore we must call main again.
        # e.__class__ is used to get the class of the exception
        if issubclass(e.__class__, Exception):  # Checks if the exception is anything but KeyBoardInterrup or SytemExit
            logging.warning('Following exception ocurred, program will try to rerun:')
            time.sleep(10) # In case there is a problem with the website, we wait 10 seconds before reruning
            main()
        # Case where the exception is KeyBoardInterrup or SytemExit
        logging.exception("Program will exit with following exception:")

    # Just in case other exception happens, we ensure the data is saved in a backup file that we will rewrite every time
    finally:
        save_csv(df_parking_data, 'last_run_backup.csv', UNMERGED_DATA_DIR_PATH, backup=True)

if __name__ == '__main__':
    main()
