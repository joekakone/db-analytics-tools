# coding : utf-8

"""
    DB Analytics Tools : Web User Interface

    This module provides classes and functions for database interactions and data migration.
"""


#####################################################################################################
import os
import argparse
import subprocess
import tempfile
import datetime
#####################################################################################################


# #####################################################################################################################################
# # DB Analytics CLI  Class
# #####################################################################################################################################
def main():
    """
    Main function for the DB Analytics CLI.
    A command-line interface for managing database analytics tools, including database connections,
    """
    #####################################################################################################
    parser = argparse.ArgumentParser(description="DB Analytics Tools CLI - A command-line interface for managing database analytics tools, including database connections, data migration, and analytics execution.")

    # Database Credentials
    parser.add_argument("--engine", help="Database engine (e.g., greenplum, postgres)", required=True)
    parser.add_argument("--host", help="Host", required=True)
    parser.add_argument("--port", help="Port", required=True)
    parser.add_argument("--database", help="Database", required=True)
    parser.add_argument("--username", help="Username", required=True)
    parser.add_argument("--password", help="Password", required=True)

    # Execution Parameters
    parser.add_argument("--start", help="Start date. e.g. 3 for D-3", default=1, required=True)
    parser.add_argument("--stop", help="Stop date. e.g. 1 for D-1", default=1, required=True)
    parser.add_argument("--freq", help="Execution frequency", required=True)
    parser.add_argument("--functions", nargs='+', help="List of functions to run", required=True)

    args = parser.parse_args()
    #####################################################################################################


    #####################################################################################################
    # Database Credentials
    engine = args.engine
    host = args.host
    port = args.port
    database = args.database
    user = args.username
    password = args.password

    # Execution Parameters
    start = args.start
    stop = args.stop
    freq = args.freq
    functions = args.functions
    #####################################################################################################


    #####################################################################################################
    # Compute start and stop dates
    #####################################################################################################
    if freq.upper() == 'D':
        start_date = (datetime.date.today() - datetime.timedelta(days=int(start))).strftime("%Y-%m-%d")
        stop_date = (datetime.date.today()  - datetime.timedelta(days=int(stop))).strftime("%Y-%m-%d")
    elif freq.upper() == 'H':
        start_date = (datetime.datetime.now() - datetime.timedelta(hours=int(start))).strftime("%Y-%m-%d %H:%M:%S")
        stop_date = (datetime.datetime.now()  - datetime.timedelta(hours=int(stop))).strftime("%Y-%m-%d %H:%M:%S")
    elif freq.upper() == 'M':
        start_date = (datetime.date.today() - datetime.timedelta(days=int(start) * 30)).strftime("%Y-%m-%d")
        stop_date = (datetime.date.today()  - datetime.timedelta(days=int(stop) * 30)).strftime("%Y-%m-%d")
    elif freq.upper() == 'W':
        start_date = (datetime.date.today() - datetime.timedelta(weeks=int(start))).strftime("%Y-%m-%d")
        stop_date = (datetime.date.today()  - datetime.timedelta(weeks=int(stop))).strftime("%Y-%m-%d")
    else:
        # raise ValueError("Invalid frequency. Use 'D' for daily, 'M' for monthly, or 'W' for weekly.")
        start_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        stop_date = (datetime.date.today()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    #####################################################################################################


    #####################################################################################################
    # Script content to initialize the app and run UI.start
    scheduler_script = f"""
#####################################################################################################
import db_analytics_tools.integration as dbi
import db_analytics_tools as db
#####################################################################################################


#####################################################################################################
client = db.Client(
    host='{host}',
    port='{port}',
    database='{database}',
    username='{user}',
    password='{password}',
    engine='{engine}'
)
etl = dbi.ETL(client)
#####################################################################################################


#####################################################################################################
# Execution
etl.run_multiple(functions={functions}, start_date='{start_date}', stop_date='{stop_date}', freq='{freq}')
#####################################################################################################
"""
    #####################################################################################################


    #####################################################################################################
    # Create a temporary Python file for the Streamlit app
    with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as temp_file:
        temp_file.write(scheduler_script.encode("utf-8"))
        temp_filename = temp_file.name
    #####################################################################################################


    #####################################################################################################
    # Run Streamlit with the temporary file and pass the arguments
    #####################################################################################################
    try:
        # Run Streamlit with the temporary file
        subprocess.run([
            "python3", temp_filename,
            "--engine", engine,
            "--host", host,
            "--port", str(port),
            "--database", database,
            "--username", user,
            "--password", password,
            "--start", start,
            "--stop", stop,
            "--freq", freq,
            "--functions", " ".join(functions),
        ], check=True)
    finally:
        # Clean up the temporary file after execution
        os.remove(temp_filename)
    #####################################################################################################


if __name__ == "__main__":
    main()
