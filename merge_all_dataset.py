import os

import pandas as pd
from dotenv import load_dotenv

import db
from utils.constants import DOTENV_PATH, DATA_DIR_PATH

def main():
    load_dotenv(DOTENV_PATH)
    query = "SELECT * FROM raw"
    print('Trying to connect...')
    df = pd.read_sql_query(query, con=db.engine)
    print('Requested data retrieved... Saving to csv')
    # df = pd.read_sql_query(query, con=db.engine, index_col="id")
    df.to_csv(DATA_DIR_PATH / 'curated/all_data.csv', sep=';')
    print('Conexion finished!')
    print(df.head())
    print(df.tail())


if __name__ == "__main__":
    main()
