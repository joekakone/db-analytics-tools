
"""
>>> import db_analytics_tools as db
>>> host = "localhost"
>>> port = "1433"
>>> database = "AdventureWorksDW2022"
>>> username = "sa"
>>> password = "1234"
>>> client = db.Client(host=host, port=port, database=database, username=username, password=password, engine="sqlserver")
"""


from .utils import Client
# from . import analytics, integration

__version__ = "0.1.3"



