# Parking-tracker-deusto

Repository for an applicaction that automatically deploys a PostgreSQL database that will store the occupation data from Deusto University parking, from
6:30 to 20:30. Data will be automatically retrieved from a website using crontab scheduling.

## Requirements

Code running the retrieval of information is designed to be in a linux machine with the following packages installed:

- docker
- docker-compose
- cron

## Instalation

1. Run ```pip install -r requirements.txt```

2. Rename the ```.env_example``` to ```.env``` in both ```docker``` and ```config``` directories and add the desired variable values. The HOST in the ```config/.env``` refers to the machine where the data is stored with respect to the machine the code is executed from. For example, you may have the docker running on a RPi on the network (e.g. 192.168.1.17) and want to access data from your local machine, so in ```.env``` make ```HOST=192.168.1.17```

3. (optional) In case you want to use PostgreSQL to manage the data, run the shell script deploy_db.sh

    - Uses docker-compose to create the DB and PgAdmin
    <!-- Work in progress 
     - Automatically downloads data from previous days and adds to raw db
     - Shedules a crontab job that retrieves occupation from the parking every 15 secs, from 6:30 to 20:30
    - Schedules another job that passes all the data to the db when daily retrieval is finished -->

4. Add to crontab using ````crontab -e```` the following lines:

    ````nano
    30 6 * * * /path-to-code-folder/venv/bin/python3 /path-to-code-folder/data_retrieval_to_csv.py
    30 20 * * * /usr/bin/pkill -f "/path-to-code-folder/venv/bin/python3 /path-to-code-folder/data_retrieval_to_csv.py"
    32 20 * * * /path-to-code-folder/venv/bin/python3 /path-to-code-folder/merge_today_csv_and_send_to_db.py
    ````

5. In case PostgreSQL is deployed, go to the port 5050 to access PgAdmin and add the server:

    ```markdown
    Name: desired-name
    Hostname: local_pgdb
    Port: 5432
    Username: $POSTGRES_USER
    Password: $POSTGRES_PASSWORD
    ```

## TODOs

1. Modify the retrieval of the parking data to consistently retrieve every 15 seconds. To do so, use the ```RepeatedTimer``` object in ```collect_data_to_csv_and_influx.py```

2. Rearrange files in a more pythonic way, meeting the following structure:

    ```bash
    .
    ├── config
    ├── Data/
    │   ├── curated/
    │   │   ├── dbs_parking.csv
    │   │   └── general_parking.csv
    │   ├── raw_merged
    │   └── raw_not_merged
    ├── docker
    ├── logs
    ├── parking_tracker/
    │   ├── data/
    │   │   ├── create_curated_dataset.py
    │   │   ├── merge_csvs.py
    │   │   └── data_retrieval.py
    │   ├── db/
    │   │   └── (db-related-scripts)
    │   ├── plots
    │   ├── utils
    │   └── __init__.py
    ├── .gitignore
    ├── LICENSE
    ├── train.py
    └── deploy_db.sh
    ```

## To make modifications

They way paths are programmed, the working directory MUST ALWAYS be the parent of utils. If you wish to change this behavior, go to constants and modify the working directory selection
