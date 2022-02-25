import os
import requests
from bs4 import BeautifulSoup
import sched, time
from datetime import datetime
import pytz
from pandas import DataFrame


# Functions
def get_general_DBS_parking_free_slots(dataframe, var_name):
    page = requests.get('http://parking.deusto.es/')
    now = datetime.now(pytz.timezone('Europe/Madrid'))
    print(now.time())
    #Get HTML content
    soup = BeautifulSoup(page.text, 'html.parser')
    n = 0
    general_number = ''
    dbs_number = ''
    for img_tag_name in soup.find_all('img'):
        if 'images/number' in img_tag_name['src'] and n<3:
          general_number = general_number + img_tag_name['src'][13]
          n += 1
        elif 'images/number' in img_tag_name['src'] and n>=3:
          dbs_number = dbs_number + img_tag_name['src'][13]
    general_number = int(general_number)
    dbs_number = int(dbs_number)
    dataframe.loc[dataframe['Time'].size] = {'Date': now.date(),
                                                   'Time': now.time(),
                                                   'General': general_number,
                                                   'DBS': dbs_number
                                                   }
    s.enter(5, 1, get_general_DBS_parking_free_slots, (globals()[var_name],var_name))


def save_csv(parking_data, var_name, dir_name, backup=False):
    if backup is True:
        var_path = os.path.join(dir_name, var_name + '.csv')
        parking_data.to_csv(var_path, sep=';', decimal=',')
        print(f'Backup of the last run saved in {var_name}.csv')
    else:
        list_existing_today_files = os.listdir(dir_name)
        coincidences = []
        var_path = ''
        string_found = -1
        # Check in the directory if there is a file of the same day already
        for item in list_existing_today_files:
            string_found = int(item.rfind(var_name + '_')) # 0 if found, -1 if not found
            if string_found == 0:
                pos = len(var_name) + 1
                coincidences.append(int(item[pos]))
            else:
                var_path = f'{var_name}_0.csv'
        # If there are coincidences, we sum 1 to dont overwrite the .csv of that day
        if len(coincidences) > 0:
            var_path = var_name + '_' + str(max(coincidences)+1) + '.csv'
        elif len(coincidences) == 0:
            # If coincidences = [], that means either that there are not files in the directory or that the
            # files in the directory are of other days
            var_path = f'{var_name}_0.csv'
        # Save to a .csv the data generated
        var_path = os.path.join(dir_name,var_path)
        parking_data.to_csv(var_path, sep=';', decimal=',')
        print(f'{var_path} saved!')

def main():
    global s
    # Constant definition
    log = []
    today = datetime.now(pytz.timezone('Europe/Madrid'))
    DIR_NAME = 'Data'
    varName = 'dataframe' + '_' + str(today.year) + '_' + str(today.month) + '_' + str(today.day)
    globals()[varName] = DataFrame(columns=['Date', 'Time', 'General', 'DBS'])
    s = sched.scheduler(time.time, time.sleep)
    # Enter the parameters to the s object
    s.enter(5, 1, get_general_DBS_parking_free_slots, (globals()[varName],varName))
    try:
        s.run()
    except BaseException as e:
        print(f'Exception {e.__class__} ocurred while retrieving data')
        # Hay que hacer que los csv vayan a unca carpeta
        # Queda por añadir un if por si me falla a mitad de dia... hacer pruebas con archivo dimmie
        # Hay que añadir a gitignore que no suba los .csv
        save_csv(globals()[varName], varName, DIR_NAME)
        # Re run main() if the error , only exit the program manually
        if issubclass(e.__class__, Exception):
            log.append(e.__class__)
            main()


    finally:
        save_csv(globals()[varName], 'last_run_backup', DIR_NAME, backup=True)
        print('The programm has been interrupted with these exceptions:')
        for error in log:
            print(error)

if __name__ == '__main__':
    main()
