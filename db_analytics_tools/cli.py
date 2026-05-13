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

# import db_analytics_tools as db
# import db_analytics_tools.integration as dbi
#####################################################################################################


# #####################################################################################################################################
# # DB Analytics CLI  Class
# #####################################################################################################################################
# class DBAnalyticsCLI:
#     """
#     A command-line interface for managing database analytics tools, including database connections,
#     """
#     def __init__(self, client):
#         self.client = client
#         self.etl = dbi.ETL(client)
        
#     def run_etl(self, functions, start_date, stop_date, frequency):
#         self.etl.run_multiple(
#             functions=functions, 
#             start_date=start_date, 
#             stop_date=stop_date, 
#             freq=frequency
#         )

def main():
    #####################################################################################################
    parser = argparse.ArgumentParser(description="Outil de planification de jobs DB Analytics")
    
    # Database Credentials
    parser.add_argument("--engine", help="Database engine (e.g., greenplum, postgres)", required=True)
    parser.add_argument("--host", help="Host", required=True)
    parser.add_argument("--port", help="Port", required=True)
    parser.add_argument("--database", help="Database", required=True)
    parser.add_argument("--username", help="Username", required=True)
    parser.add_argument("--password", help="Password", required=True)
    
    # Execution Parameters
    parser.add_argument("--start", help="Start date", required=True)
    parser.add_argument("--stop", help="Stop date", required=True)
    parser.add_argument("--functions", nargs='+', help="List of functions to run", required=True)
    parser.add_argument("--frequency", help="Execution frequency", required=True)
    
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
    functions = args.functions
    frequency = args.frequency
    #####################################################################################################


    #####################################################################################################
    # Script content to initialize the app and run UI.start
    scheduler_script = f"""
import db_analytics_tools.integration as dbi
import db_analytics_tools as db
    
    
    #####################################################################################################
    client = db.Client(
        host={host},
        port={port},
        database={database},
        username={user},
        password={password},
        engine={engine}
    )
    etl = dbi.ETL(client)
    #####################################################################################################
    
    
    #####################################################################################################
    # Execution
    etl.run_multiple(functions={functions}, start_date={start}, stop_date={stop}, freq={frequency})
#####################################################################################################
"""
    #####################################################################################################
    
    
    #####################################################################################################
    # Create a temporary Python file for the Streamlit app
    with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as temp_file:
        temp_file.write(scheduler_script.encode("utf-8"))
        temp_filename = temp_file.name

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
            "--functions", " ".join(functions),
            "--frequency", frequency
        ])
    finally:
        # Clean up the temporary file after execution
        os.remove(temp_filename)


if __name__ == "__main__":
    main()
