# coding : utf-8

'''
    Job Run Class
'''

import datetime

import psycopg2
import pandas as pd


class JobRunner:
    """SQL Based ETL Runner"""
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connect()

    def connect(self):
        """Connection to database"""
        self.conn = psycopg2.connect(host=self.host,
                                     port=self.port,
                                     database=self.database,
                                     user=self.username,
                                     password=self.password)
        self.cursor = self.conn.cursor()
        print('Connection etablished successfully !')


    def run(self, function, start_date, stop_date):
        print(f'Function   : {function}')

        # Generate Dates Range
        dates_ranges = dates_ranges = list(pd.date_range(start=start_date, end=stop_date, freq='d'))
        dates_ranges = [dt.strftime('%Y-%m-%d') for dt in dates_ranges]
        
        print(f'Date Range : From {dates_ranges[0]} to {dates_ranges[-1]}')
        print(f'Iterations : {len(dates_ranges)}')

        # Send query to server
        for date in dates_ranges:
            print(f'[{date}] ', end='')
            query = f"select {function}('{date}'::date);"
            duration = datetime.datetime.now()
            self.cursor.execute(query=query)
            self.conn.commit()
            duration = datetime.datetime.now() - duration
            print(duration)


    def close(self):
        # Close connection
        self.cursor.close()
        self.conn.close()
