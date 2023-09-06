# coding : utf-8

"""
>>> ## Import DB Analytics Tools
>>> import db_analytics_tools.integration as dbi
>>>
>>> # Database config
>>> HOST = "localhost"
>>> PORT = "5432"
>>> DATABASE = "postgres"
>>> USER = "postgres"
>>> PASSWORD = "admin"
>>> ENGINE = "postgres"
>>>
>>> ## Setup ETL
>>> etl = dbi.ETL(host=HOST, port=PORT, database=DATABASE, username=USER, password=PASSWORD, engine=ENGINE)
>>>
>>> ## ETL Functions
>>> FUNCTIONS = [
...     "public.fn_test",
...     "public.fn_test_long",
...     "public.fn_test_very_long"
... ]
>>>
>>> ## Dates to run
>>> START = '2023-08-01'
>>> STOP = '2023-08-05'
>>>
>>> # Run ETLs
>>> etl.run_multiple(functions=FUNCTIONS, start_date=START, stop_date=STOP, freq='d', reverse=False)
>>>
"""

import datetime

import pandas as pd

from db_analytics_tools import Client


class ETL(Client):
    """SQL Based ETL

    >>> ## Import DB Analytics Tools
    >>> import db_analytics_tools.integration as dbi
    >>>
    >>> # Database config
    >>> HOST = "localhost"
    >>> PORT = "5432"
    >>> DATABASE = "postgres"
    >>> USER = "postgres"
    >>> PASSWORD = "admin"
    >>> ENGINE = "postgres"
    >>>
    >>> ## Setup ETL
    >>> etl = dbi.ETL(host=HOST, port=PORT, database=DATABASE, username=USER, password=PASSWORD, engine=ENGINE)
    >>>
    >>> ## ETL Functions
    >>> FUNCTIONS = [
    ...     "public.fn_test",
    ...     "public.fn_test_long",
    ...     "public.fn_test_very_long"
    ... ]
    >>>
    >>> ## Dates to run
    >>> START = '2023-08-01'
    >>> STOP = '2023-08-05'
    >>>
    >>> # Run ETLs
    >>> etl.run_multiple(functions=FUNCTIONS, start_date=START, stop_date=STOP, freq='d', reverse=False)
    >>>
    """

    def __init__(self, host, port, database, username, password, engine="postgres"):
        super().__init__(host, port, database, username, password, engine=engine)

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
            raise NotImplemented("Frequency not supported !")

        # Reverse
        if reverse:  # Recent to Old
            dates_ranges.sort(reverse=True)

        print(f'Date Range  : From {dates_ranges[0]} to {dates_ranges[-1]}')
        print(f'Iterations  : {len(dates_ranges)}')

        return dates_ranges

    def run(self, function, start_date, stop_date, freq='d', reverse=False):
        print(f'Function    : {function}')

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to server
        for date in dates_ranges:
            print(f"[Runing Date: {date}] [Function: {function}] ", end="", flush=True)

            query = f"select {function}('{date}'::date);"
            duration = datetime.datetime.now()

            try:
                self.connect()
                self.cursor.execute(query)
                self.conn.commit()
            except Exception as e:
                raise Exception("Something went wrong !")
            finally:
                self.close()

            duration = datetime.datetime.now() - duration
            print(f'Execution time: {duration}')

    def run_multiple(self, functions, start_date, stop_date, freq='d', reverse=False):
        print(f'Functions   : {functions}')

        # Compute MAX Length of functions (Adjust display)
        max_fun = max(len(function) for function in functions)

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to server
        for date in dates_ranges:
            for function in functions:
                print(f"[Runing Date: {date}] [Function: {function.ljust(max_fun, '.')}] ", end="", flush=True)

                query = f"select {function}('{date}'::date);"
                duration = datetime.datetime.now()

                try:
                    self.connect()
                    self.cursor.execute(query)
                    self.conn.commit()
                except Exception as e:
                    raise Exception("Something went wrong !")
                finally:
                    self.close()

                duration = datetime.datetime.now() - duration
                print(f'Execution time: {duration}')
