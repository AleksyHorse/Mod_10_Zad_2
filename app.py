import csv, os, sys
from sqlalchemy import String, Float, Table, Column, Integer, String, MetaData, create_engine

def convertor_to_dict(file_path):
    with open(os.path.join(sys.path[0], file_path), "r") as inp:
        reader = csv.reader(inp)
        a=0
        out=[]
        for row in reader:
            if a == 0:
                tag_list=row
            if a == 1:
                dict_={}
                i=0
                for tag in tag_list:
                    dict_.setdefault(tag, row[i])
                    i+=1
                out.append(dict_)
            a=1
        return out
print(convertor_to_dict("clean_stations.csv"))

engine = create_engine('sqlite:///database.db', echo=True)
meta = MetaData()
stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String)
)

meta.create_all(engine)

ins = stations.insert().values(station='USC00519397', latitude= '21.2716', longitude= '-157.8168', elevation= '3.0', name= 'WAIKIKI 717.2', country= 'US', state= 'HI')

with engine.connect() as conn:
    result = conn.execute(ins)
    conn.execute(ins, (convertor_to_dict("clean_stations.csv")[1:]))

measure = Table(
   'measure', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', String),
   Column('tobs', String)
)

meta.create_all(engine)

ins = measure.insert().values(station='USC00519397',date='2010-01-01',precip='0.08',tobs='65')

with engine.connect() as conn:
    result = conn.execute(ins)
    conn.execute(ins, (convertor_to_dict("clean_measure.csv")[1:]))
