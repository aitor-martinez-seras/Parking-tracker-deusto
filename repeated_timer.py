import threading
import os
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pytz
from pandas import DataFrame
import logging
import signal


class RepeatedTimer:
  def __init__(self, interval, function, *args, **kwargs):
    self._timer = None
    self.interval = interval
    self.function = function
    self.args = args
    self.kwargs = kwargs
    self.is_running = False
    self.next_call = time.time()
    self.start() # Se ejecuta en la creación del objeto, es el punto de entrada del objeto por así decir

  def _run(self):
    # Tras pasar el intervalo definido, se ejecuta el objeto Timer y esta funcion entra en ejecucion. Primero define
    # a False el is_running para que asi cuando vaya a la función start quede programada la siguiente iteracion.
    # Si no me equivoco, casi toodo el delay que hay se debe a lo que se tarda en cambiar el atributo y en ejecutar esta
    # la funcion start()
    self.is_running = False
    # Como se describe antes, se ejecuta este método para ir preparando ya la siguiente iteración
    self.start()
    # Por fin, la funcion que nos interesa que se repita es ejecutada.
    # TODO: Por lo que he entendido, en la propia funcion que se ejecuta en el thread es donde debería ocurrir
    #  el exception handling que hago. Ahora bien, me queda la duda de cual es el main thread, ya que lo que tarda esta
    #  funcion en ejecutarse es poquito tiempo, por lo que el programa se tiene que estar ejecutando en otro lado. Me
    #  inclino a pensar que ocurre en la llamada a threading.Timer y threading.start
    self.function(*self.args, **self.kwargs)

  def start(self):
    """
    Esta funcion lo que hace es ejecutarse una y otra vez
    :return:
    """
    if not self.is_running:
      # Calula cuando tiene que ocurrir la siguiente iteracion
      self.next_call += self.interval
      # Crea la tarea de ejecutar el self._run para dentro del tiempo especificado en interval
      # (a través de self.next_call)
      try:
        self._timer = threading.Timer(self.next_call - time.time(), self._run)
        # No puedo usar esto del daemon ya que si python detecta que no hay nada corriendo que no sea un daemon se para
        #self._timer.daemon = True
        # La iteracion que ha sido programada debe ser iniciada, por lo que se usa su método .start()
        self._timer.start()
      except BaseException as e:
          print("Estoy en la exception")
          print(e)
          print(e.__class__)
          print(self.args)
      # Define que se esta corriendo codigo tras activar la siguiente iteracion (que será dentro de self.interval
      # segundos) con el método start
      self.is_running = True

  def stop(self):
    self._timer.cancel()
    self.is_running = False


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
    n = 0 # Don't remember why I have to count
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
    df_parking_data.loc[df_parking_data['Time'].size] = {'Date': now.date(),
                                             'Time': now.time(),
                                             'General': general_number,
                                             'DBS': dbs_number}
    print(df_parking_data)


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
    # SIGTERM signal handler (executes when SIGTERM (a class of signal) is issued)
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

    def sigint_handler(signum, frame):
        # TODO: Aqui es donde tiene que ir todo el codigo de guardado de dataframe. En el codigo del otro sigterm
        #  handler tambien debe ir algo nuevo, revisar
        print("Inside the signal handler")
        print(df_parking_data)
        print("Goodbye")
        rt.stop()

    signal.signal(signal.SIGINT, sigint_handler)

    # def custom_hook(args):
    #     print("Estoy en el hook")
    #     print(df_parking_data)
    #     print("Hago cosas dentro del hook")
    #     print(args)
    # threading.excepthook = custom_hook

    # Make scheduler, the DELAY and the working_dir_path constants global
    global working_dir_path
    # Define the delay (in seconds)
    DELAY = 15
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
    try:
        # Here we define the class that handles the execution of the repeated function.
        # It auto-starts, no need of rt.start()
        rt = RepeatedTimer(DELAY, get_general_DBS_parking_free_slots, df_parking_data, file_name)
        print("Before join")
        rt._timer.join()
        print("After join")
    except BaseException as e:
        rt.stop()
        print(df_parking_data)
        # In case we have an exception of any kind, we want to save the retrieved data to a .csv file if the dataframe
        # is not going to be empty
        if len(df_parking_data.index) > 2: # save .csv only if we have meaningful data
            save_csv(df_parking_data, file_name, DIR_NAME)
        # If the exception is not a KeyBoardInterrup nor a SytemExit, that do not inherit from Exception,
        # program must continue and therefore we must call main again
        # e.__class__ is used to get the class of the exception
        if issubclass(e.__class__, Exception):
            logging.warning('Following exception occurred, program will try to rerun:')
            time.sleep(10)  # In case there is a problem with the website, we wait 10 seconds before rerunning
            main()
        # If we don't return to main, we exit the program
        logging.exception("Program will exit with following exception:")
        print('Executed code till this line')

    # Just in case other exception happens, we ensure the data is saved in a backup file that we will rewrite every time
    finally:
        print('This executes')
        save_csv(df_parking_data, 'last_run_backup.csv', DIR_NAME, backup=True)


if __name__ == '__main__':
    main()