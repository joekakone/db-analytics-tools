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
    def generate_date_range(start_date, stop_date=None, freq=None, dates=None, reverse=False, streamlit=False):
        """
        Generate a range of dates.

        :param start_date: The start date for the range.
        :param stop_date: The stop date for the range.
        :param freq: The frequency of the dates ('d' for daily, 'm' for monthly).
        :param dates: A list of dates
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :param streamlit: If True, use Streamlit for progress updates.01
        :return: A list of formatted date strings.
        """
        a = start_date and (start_date or stop_date) and freq and dates is None # Date Range
        b = start_date is None and stop_date is None and freq is None and dates # Specific list of dates
        assert (a and not b) or (not a and b)
        
        if dates:
            dates_ranges = dates
            # Reverse
            if reverse:  # Recent to Old
                dates_ranges.sort(reverse=True)

            print(f'Date Range  : From {dates_ranges[0]} to {dates_ranges[-1]}')
            print(f'Iterations  : {len(dates_ranges)}')

            if streamlit:
                import streamlit as st
                st.markdown(f"<p>Date Range  : From {dates_ranges[0]} to {dates_ranges[-1]}</p>", unsafe_allow_html=True)
                st.markdown(f"<p>Iterations  : {len(dates_ranges)}</p>", unsafe_allow_html=True)

            return dates_ranges

        if start_date and stop_date is None:
            print(f'Date        : {start_date}')
            print('Iterations  : 1')

            if streamlit:
                import streamlit as st
                st.markdown(f"<p>Date        : {start_date}</p>", unsafe_allow_html=True)
                st.markdown(f"<p>Iterations  : 1</p>", unsafe_allow_html=True)

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

        if streamlit:
            import streamlit as st
            st.markdown(f"<p>Date Range  : From {dates_ranges[0]} to {dates_ranges[-1]}</p>", unsafe_allow_html=True)
            st.markdown(f"<p>Iterations  : {len(dates_ranges)}</p>", unsafe_allow_html=True)

        return dates_ranges

    def run(self, function, start_date=None, stop_date=None, freq=None, dates=None, reverse=False, streamlit=False):
        """
        Run a specified SQL function for a range of dates.

        :param function: The SQL function to run for each date.
        :param start_date: The start date for the range.
        :param stop_date: The stop date for the range.
        :param dates: A list of dates
        :param freq: The frequency of the dates ('d' for daily, 'm' for monthly).
        :param dates: A list of dates
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :param streamlit: If True, use Streamlit for progress updates.
        """
        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, dates, reverse, streamlit)
        
        print(f'Function    : {function}')

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

    def run_multiple(self, functions, start_date=None, stop_date=None, freq=None, dates=None, reverse=False, streamlit=False):
        """
        Run multiple specified SQL functions for a range of dates.

        :param functions: A list of SQL functions to run for each date.
        :param start_date: The start date for the range.
        :param stop_date: The stop date for the range.
        :param freq: The frequency of the dates ('d' for daily, 'm' for monthly).
        :param dates: A list of dates
        :param reverse: If True, the date range is generated in reverse order (from stop_date to start_date).
        :param streamlit: If True, use Streamlit for progress updates.
        """
        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, dates, reverse, streamlit)
        
        print(f'Functions   : {functions}')

        # Compute MAX Length of functions (Adjust display)
        max_fun = max(len(function) for function in functions)

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, dates, reverse)

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


def create_etl(host, port, database, username, password, engine, keep_connection):
    """
    Create an ETL (Extract, Transform, Load) instance with the specified database connection parameters.

    :param host: The hostname or IP address of the database server.
    :param port: The port number to use for the database connection.
    :param database: The name of the database to connect to.
    :param username: The username for authenticating the database connection.
    :param password: The password for authenticating the database connection.
    :param engine: The database engine to use, currently supports 'postgres' and 'sqlserver'.
    :param keep_connection: If True, the connection will be maintained until explicitly closed. If False, the connection
                           will be opened and closed for each database operation (default is False).
    :return: An ETL instance for performing data extraction, transformation, and loading.
    """
    client = Client(host=host,
                    port=port,
                    database=database,
                    username=username,
                    password=password,
                    engine=engine,
                    keep_connection=keep_connection)
    etl = ETL(client)
    return etl
