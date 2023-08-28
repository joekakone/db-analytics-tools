# coding : utf-8

import urllib

class Client:
    """SQL Based ETL Runner"""
    def __init__(self, host, port, database, username, password, engine):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.engine = engine

    def connect(self, verbose=0):
        """Connection to database"""
        if self.engine == "postgres":
            import psycopg2
            self.conn = psycopg2.connect(host=self.host,
                                         port=self.port,
                                         database=self.database,
                                         user=self.username,
                                         password=self.password)
        if self.engine == "sqlserver":
            import pyodbc
            self.conn = psycopg2.connect("Driver={SQL Server};"
                                         f"Server={self.host};"
                                         f"Database={self.database};"
                                         f"PWD={self.password}"
                                         f"UID={self.username}"
                                         "Trusted_Connection=yes;")
        else:
            raise NotImplementedError("Engine not supported")
        # Create cursor
        self.cursor = self.conn.cursor()
        if verbose == 1:
            print('Connection etablished successfully !')

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
        else:
            raise NotImplementedError("Engine not supported")
