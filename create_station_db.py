import os
from sqlalchemy import Table, Column, Date, String, MetaData, Float, Integer, inspect, create_engine
import pandas as pd

def is_table_empty(engine, table_name):
   inspector = inspect(engine)
   if table_name in inspector.get_table_names():
      with engine.connect() as connection:
         result = connection.execute(f"SELECT COUNT(*) FROM {table_name}")
         count = result.fetchone()[0]
         return count == 0
   return True

def create_db(engine):
   meta = MetaData()

   stations = Table(
      'stations', meta,
      Column('station', String, primary_key=True),
      Column('latitude', Float),
      Column('longitude', Float),
      Column('elevation', Float),
      Column('name', String),
      Column('country', String),
      Column('state', String)
   )

   measures = Table(
      'measures', meta,
      Column('station', String), 
      Column('date', Date),
      Column('precip', Float),
      Column('tobs', Integer)
   )

   meta.create_all(engine)
   return print("Database has been created")

def add_data(engine, path, table):
   df_path = pd.read_csv(path)
   with engine.connect() as connection:
      df_path.to_sql(table, connection, if_exists='append', index=False)
   return print(f'{table} data has been added')

def main(stations, measures):
   engine = create_engine('sqlite:///database.db')
   db_name = "database.db"
   database_exists = os.path.exists(db_name)

   if database_exists:
      if is_table_empty(engine, 'stations'):
         add_data(engine, stations, 'stations')
      elif is_table_empty(engine, 'measures'):
         add_data(engine, measures, 'measures' )
      else: 
         print("Database with records already exists")
   else:
      create_db(engine)
      add_data(engine, stations, 'stations')
      add_data(engine, measures, 'measures' )
   
   return

if __name__ == '__main__':
   stations = input("Enter path to csv stations file: ")
   measure = input("Enter path to csv measures file: ")
   result = main(stations, measure)