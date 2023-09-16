# coding : utf-8

"""
    DB Analytics Tools Data Integration
"""


import datetime

import pandas as pd

from db_analytics_tools import Client


class ETL:
    """SQL Based ETL"""

    def __init__(self, client):
        try:
            assert isinstance(client, Client)
        except Exception:
            raise Exception("Something went wrong !")
        self.client = client

    @staticmethod
    def generate_date_range(start_date, stop_date, freq='d', reverse=False):
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

    def run(self, function, start_date, stop_date, freq='d', reverse=False, streamlit=False):
        print(f'Function    : {function}')

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to server
        for date in dates_ranges:
            print(f"[Runing Date: {date}] [Function: {function}] ", end="", flush=True)
            if streamlit:
                import streamlit as st
                st.markdown(f"<span style='font-weight: bold;'>[Runing Date: {date}] [Function: {function}] </span>",
                            unsafe_allow_html=True)

            query = f"select {function}('{date}'::date);"
            duration = datetime.datetime.now()

            try:
                self.client.execute(query)
            except Exception as e:
                raise Exception("Something went wrong !")
            finally:
                self.client.close()

            duration = datetime.datetime.now() - duration
            print(f"Execution time: {duration}")
            if streamlit:
                st.markdown(f"<span style='font-weight: bold;'>Execution time: {duration}</span>",
                            unsafe_allow_html=True)

    def run_multiple(self, functions, start_date, stop_date, freq='d', reverse=False, streamlit=False):
        print(f'Functions   : {functions}')

        # Compute MAX Length of functions (Adjust display)
        max_fun = max(len(function) for function in functions)

        # Generate Dates Range
        dates_ranges = self.generate_date_range(start_date, stop_date, freq, reverse)

        # Send query to server
        for date in dates_ranges:
            ## Show date separator line
            print("*"*(69+max_fun))
            for function in functions:
                print(f"[Runing Date: {date}] [Function: {function.ljust(max_fun, '.')}] ", end="", flush=True)
                if streamlit:
                    import streamlit as st
                    st.markdown(
                        f"<span style='font-weight: bold;'>[Runing Date: {date}] [Function: {function}] </span>",
                        unsafe_allow_html=True)

                query = f"select {function}('{date}'::date);"
                duration = datetime.datetime.now()

                try:
                    self.client.execute(query)
                except Exception as e:
                    raise Exception("Something went wrong !")
                finally:
                    self.client.close()

                duration = datetime.datetime.now() - duration
                print(f"Execution time: {duration}")
                if streamlit:
                    st.markdown(f"<span style='font-weight: bold;'>Execution time: {duration}</span>",
                                unsafe_allow_html=True)

        ## Show final date separator line
        print("*"*(69+max_fun))
