# coding : utf-8

"""
    DB Analytics Tools Data Integration
"""

import datetime

import pandas as pd

from db_analytics_tools import Client


NBCHAR = 70


class ETL:
    """
    SQL Based ETL (Extract, Transform, Load) Class

    This class provides functionality for running SQL-based ETL processes using a database client.

    :param client: An instance of the `Client` class for connecting to the database.
    """

    def __init__(self, client):
        try:
            assert isinstance(client, Client)
        except Exception:
            raise Exception("Something went wrong!")

        self.client = client

    @staticmethod
    def generate_date_range(start_date, stop_date=None, freq='d', reverse=False):
        """
        Generate a range of dates.

        :param start_date: The start date for the range.
        :param stop_date: The stop date for the range.
        :param freq: The frequency of the dates ('d' for daily, 'm' for monthly).
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :return: A list of formatted date strings.
        """
        if stop_date is None:
            print(f'Date        : {start_date}')
            print(f'Iterations  : 1')

            return [start_date]

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
            raise NotImplementedError("Frequency not supported!")

        # Reverse
        if reverse:  # Recent to Old
            dates_ranges.sort(reverse=True)

        print(f'Date Range  : From {dates_ranges[0]} to {dates_ranges[-1]}')
        print(f'Iterations  : {len(dates_ranges)}')

        return dates_ranges

    def run(self, function, start_date, stop_date=None, dates=[], freq='d', reverse=False, streamlit=False):
        """
        Run a specified SQL function for a range of dates.

        :param function: The SQL function to run for each date.
        :param start_date: The start date for the range.
        :param stop_date: The stop date for the range.
        :param freq: The frequency of the dates ('d' for daily, 'm' for monthly).
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :param streamlit: If True, use Streamlit for progress updates.
        """
        print(f'Function    : {function}')

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to the server
        for date in dates_ranges:
            print(f"[Running Date: {date}] [Function: {function}] ", end="", flush=True)
            if streamlit:
                import streamlit as st
                st.markdown(f"<span style='font-weight: bold;'>[Running Date: {date}] [Function: {function}] </span>",
                            unsafe_allow_html=True)

            query = f"select {function}('{date}'::date);"
            duration = datetime.datetime.now()

            try:
                self.client.execute(query)
            except Exception as e:
                raise Exception("Something went wrong!")
            # finally:
            #     self.client.close()

            duration = datetime.datetime.now() - duration
            print(f"Execution time: {duration}")
            if streamlit:
                st.markdown(f"<span style='font-weight: bold;'>Execution time: {duration}</span>",
                            unsafe_allow_html=True)

    def run_dates(self, functions, dates, reverse=False, streamlit=False):
        """
        Run multiple specified SQL functions for a range of dates.

        :param functions: A list of SQL functions to run for each date.
        :param dates: A list of dates
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :param streamlit: If True, use Streamlit for progress updates.
        """
        print(f'Functions   : {functions}')

        # Compute MAX Length of functions (Adjust display)
        max_fun = max(len(function) for function in functions)

        # Generate Dates Range
        dates_ranges = dates
        if reverse:
            dates_ranges = list(reversed(dates_ranges))
        
        ##
        print(f'Iterations  : {len(dates_ranges)}')

        # Send query to the server
        for date in dates_ranges:
            # Show date separator line
            print("*" * (NBCHAR + max_fun))
            for function in functions:
                print(f"[Running Date: {date}] [Function: {function.ljust(max_fun, '.')}] ", end="", flush=True)
                if streamlit:
                    import streamlit as st
                    st.markdown(
                        f"<span style='font-weight: bold;'>[Running Date: {date}] [Function: {function}] </span>",
                        unsafe_allow_html=True)

                query = f"select {function}('{date}'::date);"
                duration = datetime.datetime.now()

                try:
                    self.client.execute(query)
                except Exception as e:
                    raise Exception("Something went wrong!")
                # finally:
                #     self.client.close()

                duration = datetime.datetime.now() - duration
                print(f"Execution time: {duration}")
                if streamlit:
                    st.markdown(f"<span style='font-weight: bold;'>Execution time: {duration}</span>",
                                unsafe_allow_html=True)

        # Show final date separator line
        print("*" * (NBCHAR + max_fun))

    def run_multiple(self, functions, start_date, stop_date=None, dates=[], freq='d', reverse=False, streamlit=False):
        """
        Run multiple specified SQL functions for a range of dates.

        :param functions: A list of SQL functions to run for each date.
        :param start_date: The start date for the range.
        :param stop_date: The stop date for the range.
        :param freq: The frequency of the dates ('d' for daily, 'm' for monthly).
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :param streamlit: If True, use Streamlit for progress updates.
        """
        print(f'Functions   : {functions}')

        # Compute MAX Length of functions (Adjust display)
        max_fun = max(len(function) for function in functions)

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to the server
        for date in dates_ranges:
            # Show date separator line
            print("*" * (NBCHAR + max_fun))
            for function in functions:
                print(f"[Running Date: {date}] [Function: {function.ljust(max_fun, '.')}] ", end="", flush=True)
                if streamlit:
                    import streamlit as st
                    st.markdown(
                        f"<span style='font-weight: bold;'>[Running Date: {date}] [Function: {function}] </span>",
                        unsafe_allow_html=True)

                query = f"select {function}('{date}'::date);"
                duration = datetime.datetime.now()

                try:
                    self.client.execute(query)
                except Exception as e:
                    raise Exception("Something went wrong!")
                # finally:
                #     self.client.close()

                duration = datetime.datetime.now() - duration
                print(f"Execution time: {duration}")
                if streamlit:
                    st.markdown(f"<span style='font-weight: bold;'>Execution time: {duration}</span>",
                                unsafe_allow_html=True)

        # Show final date separator line
        print("*" * (NBCHAR + max_fun))


def create_etl(host, port, database, username, password, engine):
    """
    Create an ETL (Extract, Transform, Load) instance with the specified database connection parameters.

    :param host: The hostname or IP address of the database server.
    :param port: The port number to use for the database connection.
    :param database: The name of the database to connect to.
    :param username: The username for authenticating the database connection.
    :param password: The password for authenticating the database connection.
    :param engine: The database engine to use, currently supports 'postgres' and 'sqlserver'.
    :return: An ETL instance for performing data extraction, transformation, and loading.
    """
    client = Client(host=host,
                    port=port,
                    database=database,
                    username=username,
                    password=password,
                    engine=engine)
    etl = ETL(client)
    return etl
