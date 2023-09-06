# DB Analytics Tools
DB Analytics Tools is a micro-framework that helps data analysts to work with data warehouses. Python enthousiast. Python provide packages to interact with databases, I've worked on a simple way to run SQL based-ETL using Python, I called it **SQL ETL Runner**, connect to db and run queries !

<!--
## Why adopt DB Analytics Tools ?
- o work. All plotting functions use a consistent tidy input data format.
- Friendly: Create pretty charts with very little customization required.
- Simple API: We've attempted to make the API as intuitive and easy to learn as possible.
- Flexibility: Chartify is built on top of Bokeh, so if you do need more control you can always fall back on Bokeh's API.
-->

## Install DB Analytics Tools
```sh
pip install db-analytics-tools
```

## Get Started

### Import DB Analytics Tools
```python
import db_analytics_tools.integration as dbi
```

### Database config
```python
HOST = "localhost"
PORT = "5432"
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "admin"
ENGINE = "postgres"
```

### Setup ETL
```python
etl = dbi.ETL(host=HOST, port=PORT, database=DATABASE, username=USER, password=PASSWORD, engine=ENGINE)
```

### Define ETL Function
```python
FUNCTION = "public.fn_test"
```

### Dates to run
```python
START = "2020-01-01"
STOP = "2020-01-31"
```

### Run ETL
```python
etl.run(function=FUNCTION, start_date=START, stop_date=STOP, freq="d", reverse=False)
```

## Docs
Documentation available on [dbanalyticstools.readthedocs.io](dbanalyticstools.readthedocs.io)

## Contributing
[See the contributing docs.](CONTRIBUTING.md)

## Maintainer
DB Analytics Tools has been developed by [Joseph Konka](https://www.linkedin.com/in/joseph-koami-konka/), a Data Science Professional. 

## Let's get in touch
[![Github Badge](https://img.shields.io/badge/-Github-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/joekakone)](https://github.com/joekakone) [![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/joseph-koami-konka/)](https://www.linkedin.com/in/joseph-koami-konka/) [![Twitter Badge](https://img.shields.io/badge/-Twitter-blue?style=flat-square&logo=Twitter&logoColor=white&link=https://www.twitter.com/joekakone)](https://www.twitter.com/joekakone) [![Gmail Badge](https://img.shields.io/badge/-Gmail-c14438?style=flat-square&logo=Gmail&logoColor=white&link=mailto:joseph.kakone@gmail.com)](mailto:joseph.kakone@gmail.com)