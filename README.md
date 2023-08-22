# SQL ETL Runner

Hello, I'm [Joseph Konka](https://www.linkedin.com/in/joseph-koami-konka/), Python enthousiast. Python provide packages to interact with databases, I've worked on a simple way to run SQL based-ETL using Python, I called it **SQL ETL Runner**, connect to db and run queries !

## Install DB Analytics Tools
```sh
pip install db_analytics_tools
```

## Get Started

### Import DB Analytics Tools
```python
import db_analytics_tools.analytics as db
```

### Database config
```python
HOST = "127.0.0.1"
PORT = 5432
DATABASE = "datawarehouse"
USER = "admin"
PASSWORD = "admin"
```

### Database config
```python
runner = db.JobRunner(
    host=HOST, 
    port=PORT, 
    database=DATABASE, 
    username=USER, 
    password=PASSWORD
)
```

### Define Function & Dates
```python
FUNCTION = "fn_daily_sales"

## Dates to run
START = "2020-01-01"
STOP = "2020-01-31"
```

### Run queries
```python
runner.run(function=FUNCTION, start_date=START, stop_date=STOP)
```

## Let's get in touch
[![Github Badge](https://img.shields.io/badge/-Github-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/joekakone)](https://github.com/joekakone) [![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/joseph-koami-konka/)](https://www.linkedin.com/in/joseph-koami-konka/) [![Twitter Badge](https://img.shields.io/badge/-Twitter-blue?style=flat-square&logo=Twitter&logoColor=white&link=https://www.twitter.com/joekakone)](https://www.twitter.com/joekakone) [![Gmail Badge](https://img.shields.io/badge/-Gmail-c14438?style=flat-square&logo=Gmail&logoColor=white&link=mailto:joseph.kakone@gmail.com)](mailto:joseph.kakone@gmail.com)