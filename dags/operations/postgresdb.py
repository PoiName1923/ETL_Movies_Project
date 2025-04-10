import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from contextlib import contextmanager

@contextmanager
def connect_postgres(username:str = 'ndtien2004', password:str = 'ndtien2004', host:str =  'postgres', port:str = "5432", db:str = 'postgres'):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname = db,
            user = username,
            password = password,
            host = host,
            port = port
        )
        print("Connect to Postgres Successfully")
        yield conn
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
            print("Stop Connect to Postgres")

class Postgres_Operation:
    '''Init'''
    def __init__(self, conn):
        self.conn = conn

    '''Execute Query'''
    def execute_query(self, query, params=None):
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            self.conn.commit()

    '''Write data'''
    def insert_data(self, table_name, df : pd.DataFrame, batch_size=1000):
        
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        with self.conn.cursor() as cursor:
            execute_batch(cursor, query, df.itertuples(index=False, name=None), page_size=batch_size)
            self.conn.commit()

    '''Read data'''
    def read_data(self, table_name, columns="*", conditions=None):
        query = f"SELECT {columns} FROM {table_name}"
        if conditions:
            query += f" WHERE {conditions}"
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()