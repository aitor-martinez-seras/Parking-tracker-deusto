from datetime import datetime
import pytz
from pandas import DataFrame
import logging
import signal
import os
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from utils.repeated_timer import RepeatedTimer
from utils.data_to_influxdb import data_to_influx
from utils.constants import DELAY


def data_to_dataframe(df_parking_data: DataFrame, timestamp: datetime, general_number: int, dbs_number: int):
    # Append elements to the Dataframe
    df_parking_data.loc[df_parking_data['Time'].size] = {'Date': timestamp.date(),
                                                         'Time': timestamp.time(),
                                                         'General': general_number,
                                                         'DBS': dbs_number}


def get_general_DBS_parking_free_slots():
    """
    Function that scrapes the webpage of the parking, collects the parking occupation and returns the .
    :return: returns the timestamp
    """
    try:
        # Scrape the page where info is available
        page = requests.get('http://parking.deusto.es/')
        timestamp = datetime.now(pytz.timezone('Europe/Madrid'))
        logging.info(timestamp.time())

        # Get HTML content
        soup = BeautifulSoup(page.text, 'html.parser')
        n = 0 # Don't remember why I have to count
        # Initialize the string variables where the number of spaces available in the General and the DBS parking
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
        # TODO: Error preventing of possible fails when general number and dbs number are not scrapped corretly,
        #  we need to know if the error comes from bad scrapping
        general_number = int(general_number)
        dbs_number = int(dbs_number)

        return timestamp, general_number, dbs_number


def collect_new_data_point(df_parking_data: DataFrame):
    """
    Scrapes a new data point and saves it to the .csv and influxdb
    :param df_parking_data: dataframe with all data points collected
    :return:
    """
    # TODO: after running the script for a period, check the Exceptions caught to update this clauses to catch the
    #  exact exceptions in each case.
    try:
        timestamp, general_number, dbs_number = get_general_DBS_parking_free_slots()
    except Exception as e1:
        logging.exception(f'Following exception occurred during scraping data: {e}')

    try:
        # Save the data to the dataframe
        data_to_dataframe(df_parking_data, timestamp, general_number, dbs_number)
    except Exception as e2:
        logging.exception(
            f'Following exception occurred during writing data to DataFrame, program will try to rerun: {e2}')

    try:
        # Save the data to influx database. This way, if an error happens, the program will have all data save in the
        # database. In case an exception occurs, just continue the execution
        data_to_influx(timestamp, general_number, dbs_number)
    except Exception as e3:
        logging.exception(
            f'Following exception occurred during writing data to Influxdb, program will try to rerun: {e3}')


def complete_file_name(file_name: str, data_dir_name: str) -> str:
    """
    Returns the complete file name (of the form dataframe_(year)_(month)_(day)_(number of files within one day))
    by searching trought the directory for files of the same day
    :param file_name: str containing the file name
    :param data_dir_name:  str containing the name of the directory where data is going to be stored
    :return: str with the complete file name
    """
    list_existing_files = os.listdir(working_dir_path / data_dir_name)
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


def save_csv(parking_data: DataFrame, file_name: str, data_dir_name: str, backup=False):
    """
    Saves the dataframe to the .csv file with the name file_name in the directory data_dir_name
    :param parking_data: Dataframe with the data
    :param file_name: str with the file name
    :param data_dir_name:  str containing the name of the directory where data is going to be stored
    """
    # Save to a .csv the data generated
    if backup is False:
        # When not saving a backup, we need to complete the file name with the number os .csv of the day
        file_name = complete_file_name(file_name, data_dir_name)
    # Create the file_path by appending the variables to the Path object with the "/" operator
    file_path = working_dir_path / data_dir_name / file_name
    parking_data.to_csv(file_path, sep=';', decimal=',')
    logging.info(f'{file_name} saved in {file_path}!')


def main():
    # SIGTERM signal handler (executes when SIGTERM (a class of signal) is issued). This signal is emited when pfkill
    # command is executed from the crontab scheduler
    def sigterm_handler(signal, frame):
        """
        When receiving the SIGTERM signal from the crontab job, save the .csv files
        """
        rt.stop()
        save_csv(df_parking_data, file_name, DIR_NAME)
        logging.info('Python script exiting gracefully after catching the SIGTERM signal')
        sys.exit(0)
    # Register the handler
    signal.signal(signal.SIGTERM, sigterm_handler)

    # SIGINT signal is received when user hits control+C
    def sigint_handler(signum, frame):
        """
        When receiving the SIGTINT signal from the user hitting control+C, save the .csv files
        :return:
        """
        rt.stop()
        save_csv(df_parking_data, file_name, DIR_NAME)
        logging.info('Python script exiting gracefully after catching the SIGINT signal')
        sys.exit(0)
    # Register the handler
    signal.signal(signal.SIGINT, sigint_handler)

    # Make the working_dir_path constants global
    global working_dir_path
    # Define the current directory for the paths
    working_dir_path = Path(os.path.dirname(os.path.realpath(__file__)))

    # Define logger basic configuration
    logging.basicConfig(filename= (working_dir_path /'app.log'), filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Constant definition
    today = datetime.now(pytz.timezone('Europe/Madrid'))
    DIR_NAME = 'Data'
    # Check if Data directory is created
    try:
        os.mkdir(working_dir_path / "Data")
    except FileExistsError:
        logging.info('Data directory already exist')

    # The file names will be dataframe followed by the year, month, day and a number between 00 and 99. The last number
    # is for the case where exceptions happen during executions and more than one dataframe per day is needed
    # File names -> dataframe_(year)_(month)_(day)_(number of files in current day).csv
    file_name = 'dataframe' + '_' + str(today.year) + '_' + str(today.month).zfill(2) + '_' + str(today.day).zfill(2)
    df_parking_data = DataFrame(columns=['Date', 'Time', 'General', 'DBS'])

    # We handle every exception to check if we are killing the program or some other exception happened, so in that case
    # we can call the program to restart itself
    # Here we define the class that handles the execution of the repeated function.
    # It auto-starts, no need of rt.start()
    # Because of the way the RepeatedTimer class works, if the executing thread throws an exception, the next thread
    # is already started (but it will execute when the interval has elapsed), so the program will indefinitely conitnue
    # running till the SIGTERM or SIGINT signals are issued by the OS.
    rt = RepeatedTimer(DELAY, collect_new_data_point, df_parking_data)


if __name__ == '__main__':
    main()
