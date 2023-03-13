import pandas as pd

from sqlalchemy.orm import declarative_base
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine, text


import db
from models import Entry, Base
from config import DATABASE



def main():
    # SQL queries always between double commas " "
    sql = "SELECT * FROM raw"

    # To select between dates, the date has to be in the format of the table and between commas (e.g. 'YYYY-MM-DD')
    sql = "SELECT * FROM raw WHERE date BETWEEN '2022-05-12' AND '2022-05-12'"

    # Same for the time as for the dates, use the DB format (It can be seen through pdAdmin in the case of Postgres)
    # To use multiple conditions, just put AND between the conditions, regardless of if an AND has been used 
    # in the BETWEEN clause
    # sql = "SELECT * FROM raw WHERE date BETWEEN '2022-05-12' AND '2022-05-15' AND time BETWEEN '08:30:00' AND '10:30:00'"
    # To fix AttributeError: 'OptionEngine' object has no attribute 'execute' , we have to use the connection() object
    # and the text object from SQLAlchemy, as described in:
    # https://levelup.gitconnected.com/how-to-fix-attributeerror-optionengine-object-has-no-attribute-execute-in-pandas-eb635fbb89e4
    # Error is due to the fact that 
    with db.engine.connect() as connection:
        df = pd.read_sql(text(sql), con=connection)
    
    # Convert datetime.time objects into datetime.datetime objects by adding a date
    # to the time
    datetimes = [datetime.combine(d, t) for d, t in zip(df.date, df.time)]
    fig, ax = plt.subplots(1,1, figsize=(12, 6))

    with plt.style.context('seaborn-dark-palette'):
        ax.plot(datetimes, df.general_parking)
        ax.set_title('Parking free spaces')
        ax.set_ylabel('Free spaces')
        ax.set_xlabel('Time of Day (MAD; UTC+1)')
        # Re-format the x-axis
        fmt = mdates.DateFormatter('%H:%M')
        ax.xaxis.set_major_formatter(fmt)
    plt.show(block=True)


if __name__ == "__main__":
    main() 
