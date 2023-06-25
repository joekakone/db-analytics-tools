# SQL ETL Runner

Hello, I'm [Joseph Konka](https://www.linkedin.com/in/joseph-koami-konka/), Python enthousiast. Python provide packages to interact with databases, I've worked on a simple way to run SQL based-ETL using Python, I called it **SQL ETL Runner**, connect to db and run queries !

## Install dependencies
```sh
pip install -r requirements.txt
```

## Get Started
```python
import getpass
from utils import JobRunner

# Database config
HOST = '###'
PORT = '###'
DATABASE = '###'
USER = '###'

PASSWORD = getpass.getpass('Enter your password: ')

# Database Connection
runner = JobRunner(host=HOST, port=PORT, database=DATABASE, username=USER, password=PASSWORD)

## Reporting Function
FUNCTION = '####'

## Dates to run
START = '2020-01-01'
STOP = '2020-01-31'

#  Run queries
runner.run(function=FUNCTION, start_date=START, stop_date=STOP)

# Close connection
runner.close()
```

## Let's get in touch
[![Github Badge](https://img.shields.io/badge/-Github-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/joekakone)](https://github.com/joekakone) [![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/joseph-koami-konka/)](https://www.linkedin.com/in/joseph-koami-konka/) [![Twitter Badge](https://img.shields.io/badge/-Twitter-blue?style=flat-square&logo=Twitter&logoColor=white&link=https://www.twitter.com/joekakone)](https://www.twitter.com/joekakone) [![Gmail Badge](https://img.shields.io/badge/-Gmail-c14438?style=flat-square&logo=Gmail&logoColor=white&link=mailto:joseph.kakone@gmail.com)](mailto:joseph.kakone@gmail.com)