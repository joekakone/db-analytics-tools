<div align="center">
  <img src="https://raw.githubusercontent.com/joekakone/db-analytics-tools/master/cover.png"><br>
</div>

# DB Analytics Tools
Databases Analytics Tools is a Python open source micro framework for data analytics. DB Analytics Tools is built on top of Psycopg2, Pyodbc, Pandas, Matplotlib and Scikit-learn. It helps data analysts to interact with data warehouses as traditional databases clients.


## Why adopt DB Analytics Tools ?
- Easy to learn : It is high level API and doesn't require any special effort to learn.
- Real problems solver : It is designed to solve real life problems of the Data Analyst
- All in One : Support queries, Data Integration, Analysis, Visualization and Machine Learning


## Core Components
| # | Component | Description | How to import |
-- | -- | -- | --
0 | db | Database Interactions (Client) | `import db_analytics_tools as db`
1 | dbi | Data Integration & Data Engineering | `import db_analytics_tools.integration as dbi`
2 | dba | Data Analysis | `import db_analytics_tools.analytics as dba`
3 | dbviz | Data Visualization | `import db_analytics_tools.plotting as dbviz`
4 | dbml | Machine Learning & MLOps | `import db_analytics_tools.learning as dbml`


## Install DB Analytics Tools
### Dependencies
DB Analytics Tools requires
* Python
* Psycopg2
* Pyodbc
* Pandas
* SQLAlchemy
* Streamlit

DB Analytics Tools can easily installed using pip
```sh
pip install db-analytics-tools
```


## Get Started
### Setup client
As traditional databases clients, we need to provide database server ip address and port and credentials. DB Analytics Tools supports Postgres and SQL Server.
```python
# Import DB Analytics Tools
import db_analytics_tools as db

# Database Infos & Credentials
ENGINE = "postgres"
HOST = "localhost"
PORT = "5432"
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "admin"

# Setup client
client = db.Client(host=HOST, port=PORT, database=DATABASE, username=USER, password=PASSWORD, engine=ENGINE)
```

### Data Definition Language
```python
query = """
----- CREATE TABLE -----
drop table if exists public.transactions;
create table public.transactions (
    transaction_id integer primary key,
    client_id integer,
    product_name varchar(255),
    product_category varchar(255),
    quantity integer,
    unitary_price numeric,
    amount numeric
);
"""

client.execute(query=query)
```

### Data Manipulation Language
```python
query = """
----- POPULATE TABLE -----
insert into public.transactions (transaction_id, client_id, product_name, product_category, quantity, unitary_price, amount)
values
	(1,101,'Product A','Category 1',5,100,500),
	(2,102,'Product B','Category 2',3,50,150),
	(3,103,'Product C','Category 1',2,200,400),
	(4,102,'Product A','Category 1',7,100,700),
	(5,105,'Product B','Category 2',4,50,200),
	(6,101,'Product C','Category 1',1,200,200),
	(7,104,'Product A','Category 1',6,100,600),
	(8,103,'Product B','Category 2',2,50,100),
	(9,103,'Product C','Category 1',8,200,1600),
	(10,105,'Product A','Category 1',3,100,300);
"""

client.execute(query=query)
```

### Data Query Language
```python
query = """
----- GET DATA -----
select *
from public.transactions
order by transaction_id;
"""

dataframe = client.read_sql(query=query)
print(dataframe.head())
```
```txt
   transaction_id  client_id product_name product_category  quantity  unitary_price  amount
0               1        101    Product A       Category 1         5          100.0   500.0
1               2        102    Product B       Category 2         3           50.0   150.0
2               3        103    Product C       Category 1         2          200.0   400.0
3               4        102    Product A       Category 1         7          100.0   700.0
4               5        105    Product B       Category 2         4           50.0   200.0
```

## Implement SQL based ETL
ETL API is in the integration module `db_analytics_tools.integration`. Let's import it ans create an ETL object.
```python
# Import Integration module
import db_analytics_tools.integration as dbi

# Setup ETL
etl = dbi.ETL(host=HOST, port=PORT, database=DATABASE, username=USER, password=PASSWORD, engine=ENGINE)
```

ETLs for DB Analytics Tools consists in functions with date parameters. Everything is done in one place i.e on the database. So first create a function on the database like this
```sql
create or replace function public.fn_test(rundt date) returns integer
language plpgsql
as
$$
begin
	--- DEBUG MESSAGE ---
	raise notice 'rundt : %', rundt;

	--- EXTRACT ---

	--- TRANSFORM ---

	--- LOAD ---

	return 0;
end;
$$;
```

### Run a function
Then ETL function can easily be run using the ETL class via the method `ETL.run()`
```python
# ETL Function
FUNCTION = "public.fn_test"

## Dates to run
START = "2023-08-01"
STOP = "2023-08-05"

# Run ETL
etl.run(function=FUNCTION, start_date=START, stop_date=STOP, freq="d", reverse=False)
```
```
Function    : public.fn_test
Date Range  : From 2023-08-01 to 2023-08-05
Iterations  : 5
[Runing Date: 2023-08-01] [Function: public.fn_test] Execution time: 0:00:00.122600
[Runing Date: 2023-08-02] [Function: public.fn_test] Execution time: 0:00:00.049324
[Runing Date: 2023-08-03] [Function: public.fn_test] Execution time: 0:00:00.049409
[Runing Date: 2023-08-04] [Function: public.fn_test] Execution time: 0:00:00.050019
[Runing Date: 2023-08-05] [Function: public.fn_test] Execution time: 0:00:00.108267
```

### Run several functions
Most of time, several ETL must be run and DB Analytics Tools supports running functions as pipelines.
```python
## ETL Functions
FUNCTIONS = [
    "public.fn_test",
    "public.fn_test_long",
    "public.fn_test_very_long"
]

## Dates to run
START = "2023-08-01"
STOP = "2023-08-05"

# Run ETLs
etl.run_multiple(functions=FUNCTIONS, start_date=START, stop_date=STOP, freq="d", reverse=False)
```
```
Functions   : ['public.fn_test', 'public.fn_test_long', 'public.fn_test_very_long']
Date Range  : From 2023-08-01 to 2023-08-05
Iterations  : 5
[Runing Date: 2023-08-01] [Function: public.fn_test..........] Execution time: 0:00:00.159917
[Runing Date: 2023-08-01] [Function: public.fn_test_long.....] Execution time: 0:00:00.182654
[Runing Date: 2023-08-01] [Function: public.fn_test_very_long] Execution time: 0:00:00.095138
[Runing Date: 2023-08-02] [Function: public.fn_test..........] Execution time: 0:00:00.125952
[Runing Date: 2023-08-02] [Function: public.fn_test_long.....] Execution time: 0:00:00.134392
[Runing Date: 2023-08-02] [Function: public.fn_test_very_long] Execution time: 0:00:00.112478
[Runing Date: 2023-08-03] [Function: public.fn_test..........] Execution time: 0:00:00.128775
[Runing Date: 2023-08-03] [Function: public.fn_test_long.....] Execution time: 0:00:00.111676
[Runing Date: 2023-08-03] [Function: public.fn_test_very_long] Execution time: 0:00:00.093985
[Runing Date: 2023-08-04] [Function: public.fn_test..........] Execution time: 0:00:00.114726
[Runing Date: 2023-08-04] [Function: public.fn_test_long.....] Execution time: 0:00:00.157993
[Runing Date: 2023-08-04] [Function: public.fn_test_very_long] Execution time: 0:00:00.127080
[Runing Date: 2023-08-05] [Function: public.fn_test..........] Execution time: 0:00:00.109306
[Runing Date: 2023-08-05] [Function: public.fn_test_long.....] Execution time: 0:00:00.159218
[Runing Date: 2023-08-05] [Function: public.fn_test_very_long] Execution time: 0:00:00.124130
```

## Documentation
Documentation available on [https://joekakone.github.io/db-analytics-tools-docs](https://joekakone.github.io/db-analytics-tools-docs).


## Help and Support
If you need help on DB Analytics Tools, please send me an message on [Whatsapp](https://wa.me/+22891518923) or send me a [mail](mailto:contact@josephkonka.com).


## Contributing
[Please see the contributing docs.](CONTRIBUTING.md).


## Maintainer
DB Analytics Tools is maintained by [Joseph Konka](https://www.linkedin.com/in/joseph-koami-konka/). Joseph is a Data Science Professional with a focus on Python based tools. He developed the base code while working at Togocom to automate his daily tasks. He packages the code into a Python package called **SQL ETL Runner** which becomes **Databases Analytics Tools**. For more about Joseph Konka, please visit [www.josephkonka.com](https://josephkonka.com).


## Let's get in touch
[![Github Badge](https://img.shields.io/badge/-Github-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/joekakone)](https://github.com/joekakone) [![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/joseph-koami-konka/)](https://www.linkedin.com/in/joseph-koami-konka/) [![Twitter Badge](https://img.shields.io/badge/-Twitter-blue?style=flat-square&logo=Twitter&logoColor=white&link=https://www.twitter.com/joekakone)](https://www.twitter.com/joekakone) [![Gmail Badge](https://img.shields.io/badge/-Gmail-c14438?style=flat-square&logo=Gmail&logoColor=white&link=mailto:joseph.kakone@gmail.com)](mailto:joseph.kakone@gmail.com)