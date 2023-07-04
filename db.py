import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

from utils.constants import DOCKER_DOTENV_PATH, DOTENV_PATH


# Creates the URL to connect to the database in a friendly way
# In this case would be: postgresql+psycopg2://postgres:944780847Aa.@localhost:5432/Parking-deusto
# Scheme: "postgres+psycopg2://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>"

load_dotenv(DOCKER_DOTENV_PATH)
load_dotenv(DOTENV_PATH)
HOST = os.environ['HOST'] 
USERNAME = os.environ['POSTGRES_USER']
PASSWORD = os.environ['POSTGRES_PASSWORD']
PORT = os.environ['POSTGRES_PORT']
DATABASE = os.environ['POSTGRES_DB']

uri = URL.create(
    drivername="postgresql+psycopg2",
    username=USERNAME,
    host=HOST,
    port=PORT,
    database=DATABASE,
    password=PASSWORD,
)

engine = create_engine(uri)

Session = sessionmaker(bind=engine)

# Base = declarative_base()

# In order to use the context manager to manage the sessions, this function must be created
@contextmanager
def create_temporal_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def recreate_database(base):
    base.metadata.drop_all(engine)
    base.metadata.create_all(engine)
