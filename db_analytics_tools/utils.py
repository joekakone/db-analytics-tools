# coding : utf-8

import urllib
import datetime

import pandas as pd


class Client:
    """SQL Client Class"""

    def __init__(self, host, port, database, username, password, engine="postgres"):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.engine = engine
        ### Test Connection ###
        self.test_connection()

    def connect(self, verbose=0):
        """Connection to database"""
        if self.engine == "postgres":
            import psycopg2
            self.conn = psycopg2.connect(host=self.host,
                                         port=self.port,
                                         database=self.database,
                                         user=self.username,
                                         password=self.password)
        elif self.engine == "sqlserver":
            import pyodbc
            self.conn = pyodbc.connect("Driver={SQL Server};"
                                       f"Server={self.host},{self.port};"
                                       f"Database={self.database};"
                                       f"UID={self.username};"
                                       f"PWD={self.password};"
                                       "Trusted_Connection=yes;")
        else:
            raise NotImplementedError("Engine not supported")
        # Create cursor
        self.cursor = self.conn.cursor()
        if verbose == 1:
            print('Connection established successfully !')

    def test_connection(self, verbose=1):
        try:
            self.connect(verbose=1)
            self.close()
        except Exception:
            raise Exception("Something went wrong ! Verify database infos and credentials")

    def close(self, verbose=0):
        """Close connection"""
        self.cursor.close()
        self.conn.close()
        if verbose == 1:
            print('Connection closed successfully !')

    def generate_uri(self):
        """Genrate URI"""
        password = urllib.parse.quote(self.password)
        if self.engine == "postgres":
            self.uri = f"postgresql+psycopg2://{self.username}:{password}@{self.host}:{self.port}/{self.database}"
        elif self.engine == "sqlserver":
            driver = 'ODBC Driver 17 for SQL Server'
            self.uri = f"mssql+pyodbc://{self.username}:{password}@{self.host}:{self.port}/{self.database}?driver={driver}"
        else:
            raise NotImplementedError("Engine not supported")

    def execute(self, query, verbose=0):
        duration = datetime.datetime.now()
        self.connect()
        self.cursor.execute(query)
        self.conn.commit()
        self.close()
        duration = datetime.datetime.now() - duration
        if verbose == 1:
            print(f'Execution time: {duration}')

    def read_sql(self, query, verbose=0):
        self.generate_uri()
        duration = datetime.datetime.now()
        dataframe = pd.read_sql(query, self.uri)
        duration = datetime.datetime.now() - duration
        if verbose == 1:
            print(f'Execution time: {duration}')

        return dataframe
