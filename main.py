import os
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import sched, time
from datetime import datetime
import pytz
from pandas import DataFrame
import logging


# Functions
def get_general_DBS_parking_free_slots(dataframe, var_name):
    """
    
    :param dataframe: 
    :param var_name: 
    :return: 
    """
    # Scrape the page where info is available
    page = requests.get('http://parking.deusto.es/')
    now = datetime.now(pytz.timezone('Europe/Madrid'))
    logging.info(now.time())
    #Get HTML content
    soup = BeautifulSoup(page.text, 'html.parser')
    n = 0 # Don't remember why I have to count
    general_number = ''
    dbs_number = ''
    for img_tag_name in soup.find_all('img'):
        if 'images/number' in img_tag_name['src'] and n<3:
          general_number = general_number + img_tag_name['src'][13]
          n += 1
        elif 'images/number' in img_tag_name['src'] and n>=3:
          dbs_number = dbs_number + img_tag_name['src'][13]
    # Cast to integers
    # TODO: Error preventing of possible fails when general number and dbs number are not scrapped corretly
    general_number = int(general_number)
    dbs_number = int(dbs_number)
    # Append elements to the Dataframe
    # TODO: See if this is the optimal way
    dataframe.loc[dataframe['Time'].size] = {'Date': now.date(),
                                                   'Time': now.time(),
                                                   'General': general_number,
                                                   'DBS': dbs_number
                                                   }
    # Enter again the action in the scheduler
    # TODO: Understand why it needs to be called again
    s.enter(delay=DELAY, priority=1, action=get_general_DBS_parking_free_slots, argument=(globals()[var_name],var_name))


def save_csv(parking_data, var_name, data_dir_name, backup=False):
    # To use this function for saving the backup also
    if backup is True:
        file_path = working_dir_path / data_dir_name / (var_name + ".csv")
        file_path = os.path.join(data_dir_name, var_name + '.csv')
        parking_data.to_csv(file_path, sep=';', decimal=',')
    else:
        list_existing_files = os.listdir(working_dir_path / data_dir_name)
        coincidences = []
        file_path = ''
        string_found = -1
        # Check in the directory if there is a file of the same day already
        for item in list_existing_files:
            string_found = int(item.rfind(var_name + '_')) # 0 if found, -1 if not found
            if string_found == 0:
                pos = len(var_name) + 1
                coincidences.append(int(item[pos:pos+2]))
            else:
                file_path = f'{var_name}_00.csv'
        # If there are coincidences, we sum 1 to not overwrite the .csv of that day
        if len(coincidences) > 0:
            # Extract the last item -> var_name + 'XX' and sum 1 to prevent the overwriting
            number_of_csv_today = str(max(coincidences)+1)
            if len(number_of_csv_today) == 2: # The case of two digit numbers
                file_path = var_name + '_' + number_of_csv_today + '.csv'
            elif len(number_of_csv_today) == 1: # The case of one digit numbers (we add a 0 before the number)
                file_path = var_name + '_0' + number_of_csv_today + '.csv'
            else:
                logging.info(f'Number of .csv today is {number_of_csv_today}')
        elif len(coincidences) == 0:
            # If coincidences = [], that means either that there are not files in the directory or that the
            # files in the directory are of other days
            file_path = f'{var_name}_00.csv'
        # Save to a .csv the data generated
        file_path = working_dir_path / data_dir_name / file_path
        # TODO: Disjoint the array name of the .csv name
        parking_data.to_csv(file_path, sep=';', decimal=',')
        logging.info(f'{file_path} saved!')

def main():
    # Define logger basic configuration
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    # Make scheduler, the DELAY and the working_dir_path constants global
    global s, DELAY, working_dir_path
    # Define the delay (in seconds)
    DELAY = 15
    # Define the current directory for the paths
    working_dir_path = Path(os.path.dirname(os.path.realpath(__file__)))

    # Constant definition
    log = []
    today = datetime.now(pytz.timezone('Europe/Madrid'))
    DIR_NAME = 'Data'
    # Check if Data directory is created
    try:
        os.mkdir(dir_path / "Data")
    except FileExistsError:
        logging.info('Data directory already exist')

    # TODO: The var_name is not needed to define the name of the .csv generated when saving the collected data, so
    #   in future optimization me must get rid of it, as it is not needed
    var_name = 'dataframe' + '_' + str(today.year) + '_' + str(today.month) + '_' + str(today.day)
    globals()[var_name] = DataFrame(columns=['Date', 'Time', 'General', 'DBS'])

    # Here we definde the time function and the sleep function for the scheduler
    s = sched.scheduler(time.time, time.sleep)
    # Enter the parameters to the s object
    s.enter(delay=DELAY, priority=1, action=get_general_DBS_parking_free_slots, argument=(globals()[var_name],var_name))

    # We handle every exception to check if we are killing the program or some other exception happened, so in that case
    # we can call the program to restart itself
    try:
        s.run() # Makes the scheduler start
    except BaseException as e:
        # In case we have an exception of any kind, we want to save out current data to a .csv file
        save_csv(globals()[var_name], var_name, DIR_NAME)
        if issubclass(e.__class__, Exception):
            logging.warning('Exception:')
            main()
        # If we don't return to main, we exit the program
        logging.exception("Program will exit with following exception:")

    # Just in case other exception happens, we ensure the data is saved in a backup file that we will rewrite every time
    finally:
        save_csv(globals()[var_name], 'last_run_backup', DIR_NAME, backup=True)
        logging.info("last run's backup saved correctly")

if __name__ == '__main__':
    #main()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    print(os.path.join(dir_path, "Data"))
    dir_path = Path(os.path.dirname(os.path.realpath(__file__)))
    a = "dataframe"
    print(dir_path / "Data" / (a + ".csv"))



