# coding : utf-8

'''
    Daily Job Run Class
'''


import urllib
import datetime

import psycopg2
import pandas as pd


class JobRunner:
    """SQL Based ETL Runner"""
    def __init__(self, host, port, database, username, password, engine="postgres"):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.engine = engine
        # self.connect()

    def connect(self, verbose=0):
        """Connection to database"""
        if self.engine == "postgres":
            self.conn = psycopg2.connect(host=self.host,
                                         port=self.port,
                                         database=self.database,
                                         user=self.username,
                                         password=self.password)
            self.cursor = self.conn.cursor()
        else:
            raise NotImplementedError("Engine not supported")
        if verbose == 1:
            print('Connection etablished successfully !')

    def close(self, verbose=0):
        # Close connection
        self.cursor.close()
        self.conn.close()
        if verbose == 1:
            print('Connection closed successfully !')

    def generate_uri(self):
        """Genrate URI"""
        password = urllib.parse.quote(self.password)
        self.uri = f"postgresql+psycopg2://{self.username}:{password}@{self.host}:{self.port}/{self.database}"

    def execute(self, query):
        duration = datetime.datetime.now()
        self.connect()
        self.cursor.execute(query=query)
        self.conn.commit()
        self.close()
        duration = datetime.datetime.now() - duration
        print(f'Execution time: {duration}')

    def read_sql(self, query):
        self.generate_uri()
        duration = datetime.datetime.now()
        dataframe = pd.read_sql(query, self.uri)
        duration = datetime.datetime.now() - duration
        print(f'Execution time: {duration}')
        
        return dataframe

    def generate_date_range(self, start_date, stop_date, freq='d', reverse=False):
        """Generate Dates Range"""
        dates_ranges = list(pd.date_range(start=start_date, end=stop_date, freq='d'))
        
        # Manage Frequency
        if freq.upper() == 'D':
            dates_ranges = [dt.strftime('%Y-%m-%d') for dt in dates_ranges]
        elif freq.upper() == 'M':
            dates_ranges = [
                dt.strftime('%Y-%m-%d') 
                for dt in dates_ranges if dt.strftime('%Y-%m-%d').endswith('01')
            ]
        else:
            raise NotImplemented
        
        # Reverse
        if reverse: # Recent to Old
            dates_ranges.sort(reverse=True)

        print(f'Date Range : From {dates_ranges[0]} to {dates_ranges[-1]}')
        print(f'Iterations : {len(dates_ranges)}')

        return dates_ranges

    def run(self, function, start_date, stop_date, freq='d', reverse=False):
        print(f'Function   : {function}')
        
        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to server
        for date in dates_ranges:
            print(f'[{date}] ', end='')
            query = f"select {function}('{date}'::date);"
            duration = datetime.datetime.now()
            self.connect()
            self.cursor.execute(query=query)
            self.conn.commit()
            self.close()
            duration = datetime.datetime.now() - duration
            # print(f'\tWall time: {duration}')
            print(duration)

    def run_multiple(self, functions, start_date, stop_date, freq='d', reverse=False):
        print(f'Functions   : {functions}')
        
        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to server'
        for date in dates_ranges:
            for function in functions:
                print(f'[{date}] [{function}] ', end='')
                query = f"select {function}('{date}'::date);"
                duration = datetime.datetime.now()
                self.connect()
                self.cursor.execute(query=query)
                self.conn.commit()
                self.close()
                duration = datetime.datetime.now() - duration
                # print(f'\tWall time: {duration}')
                print(duration)
