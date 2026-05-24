# coding : utf-8

"""
    DB Analytics Tools Cron Sample
"""


#####################################################################################################
import argparse

import db_analytics_tools as db
import db_analytics_tools.integration as dbi
#####################################################################################################


def main():
    #####################################################################################################
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", help="Database engine (e.g., greenplum, postgres)")
    parser.add_argument("--host", help="Host")
    parser.add_argument("--port", help="Port")
    parser.add_argument("--database", help="Database")
    parser.add_argument("--user", help="Username")
    parser.add_argument("--password", help="Password")
    parser.add_argument("--start", help="Start date")
    parser.add_argument("--stop", help="Stop date")
    parser.add_argument("--functions", nargs='+', help="List of functions to run")
    args = parser.parse_args()
    #####################################################################################################
    
    
    #####################################################################################################
    # Database config
    ENGINE = args.engine or "greenplum"
    HOST = args.host or "10.228.11.246"
    PORT = args.port or "5432"
    DATABASE = args.database or "cdrfw"
    USERNAME = args.user or "jkonka"
    PASSWORD = args.password or "Axian2024!"
    FUNCTIONS = args.functions or [
        "bibox.fn_gros_ad_lu_agents",
        "bibox.fn_gros_ad_lu_agents_month_alignement",
    ]
    START = args.start or "2026-01-01"
    STOP = args.stop or "2026-05-01"
    #####################################################################################################


    #####################################################################################################
    client = db.Client(host=HOST,
                    port=PORT,
                    database=DATABASE,
                    username=USERNAME,
                    password=PASSWORD,
                    engine=ENGINE)
    etl = dbi.ETL(client)
    #####################################################################################################


    #####################################################################################################

    # Run multiples
    etl.run_multiple(functions=FUNCTIONS, start_date=START, stop_date=STOP, freq='m')
    #####################################################################################################


if __name__ == "__main__":
    main()


"""
--- EXEMPLE D'UTILISATION ---

python3 cron_sample.py \
    --engine greenplum \
    --host 10.228.11.246 \
    --port 5432 \
    --database cdrfw \
    --user jkonka \
    --password Axian2024! \
    --start 2026-01-01 \
    --stop 2026-05-01 \
    --functions bibox.fn_gros_ad_lu_agents bibox.fn_gros_ad_lu_agents_month_alignement
"""
