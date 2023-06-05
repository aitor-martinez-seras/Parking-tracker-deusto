import pandas as pd

from datetime import datetime

import db
from models import Entry, Base  # Base must be imported from models as it must live there
from utils.constants import RAW_MERGED_DATA_DIR_PATH


# La conexion a la base de datos funciona en el ordenador incluso aunque no esté encendido PgAdmin
def add_one_entry():
    # This statement creates this table in the database if it does not exist. In case it exist,
    # it does NOT overwrite it
    

    with db.create_temporal_session() as s:
        entry = Entry(
            date=datetime.date(datetime.now()),
            time=datetime.time(datetime.now()),
            dbs_parking=11,
            general_parking=11,
        )
        s.add(entry)


def main():
    # This statement creates this table in the database if it does not exist. In case it exist,
    # it does NOT overwrite it
    # Base.metadata.create_all(db.engine)

    # To add data using SQLAlchemy and Pandas, the easiest way is to have a DF with column names
    # as the names of the columns in the database an to use the .to_sql() method
    # con must be the conexion or the engine
    # schema is needed only in the cases where it is defined in the DB (in Postgres is defined)
    # index is used to not upload the first column of indexes 
    # if_exist is used to handle the behaviour when the table you are inserting to already exist
    
    table_name = 'raw'
    schema_name = 'public'  # By default in Postgress
    for file in RAW_MERGED_DATA_DIR_PATH.iterdir():
        df = pd.read_csv(file, sep=';', index_col=0)
        # Name is the name of the DB inside the Postgres database
        df.to_sql(name=table_name, con=db.engine, schema=schema_name , index=False, if_exists='append')
        print(f'File {file} succesfully uploaded to the database {db.DATABASE}, in the table {table_name} in the schema {schema_name}')


if __name__ == "__main__":
    db.recreate_database(Base)
    main()
