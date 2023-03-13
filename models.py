from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, TIME


# It is crucial to inherit the Base class from declarative base in order to let the SQLAlchemy
# library know about the classes we have defined and to be able to perform operations like create_all
# and drop_all. I don't know why but it must be instantiated where the models live.
Base = declarative_base()


class Entry(Base):
    __tablename__ = 'raw'
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    time = Column(TIME)
    dbs_parking = Column(Integer)
    general_parking = Column(Integer)

    def __repr__(self):
        return f"<Entry (id='{self.id}', date='{self.date}', time={self.time}, DBS={self.dbs_parking}," \
               f" General={self.general_parking})>"