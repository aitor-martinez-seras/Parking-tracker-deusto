# Parking-tracker-deusto

Repository for an applicaction that automatically deploys a PostgreSQL database that will store the occupation data from Deusto University parking, from
6:30 to 20:30. Data will be automatically retrieved from a website using crontab scheduling.

## Requirements

docker
docker-compose
cron

## Instalation

Run ````pip install -r requirements.txt```

Rename the .env_example to .env and add the desired variable values

Run the shell script run.sh

1. Uses docker-compose to create the DB and PgAdmin
2. Automatically downloads data from previous days and adds to raw db
3. Shedules a crontab job that retrieves occupation from the parking every 15 secs, from 6:30 to 20:30
4. Schedules another job that passes all the data to the db when daily retrieval is finished

Go to the port 5050 to access PgAdmin and add the server:

    Name: desired-name
    Hostname: local_pgdb
    Port: 5432
    Username: $POSTGRES_USER
    Password: $POSTGRES_PASSWORD

Add to crontab using ````crontab -e```` the following lines:
````nano
30 6 * * * /path-to-code-folder/venv/bin/python3 /path-to-code-folder/data_retrieval_to_csv.py
30 20 * * * /usr/bin/pkill -f "/path-to-code-folder/venv/bin/python3 /path-to-code-folder/data_retrieval_to_csv.py"
32 20 * * * /path-to-code-folder/venv/bin/python3 /path-to-code-folder/merge_today_csv_and_send_to_db.py
````

## Modification

They way paths are programmed, the working directory MUST ALWAYS be the parent of utils. If you wish to change this behavior, go to constants and modify the working directory selection
