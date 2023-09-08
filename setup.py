# coding : utf-8

from setuptools import setup, find_packages

from db_analytics_tools import __version__

setup(
    name="db_analytics_tools",
    version=__version__,
    url="https://github.com/joekakone/db-analytics-tools",
    download_url="https://github.com/joekakone/db-analytics-tools",
    license="MIT",
    author="Joseph Konka",
    author_email="contact@josephkonka.com",
    description="Databases Tools for Data Analytics",
    keywords="databases analytics etl sql orc",
    long_description="Databases Tools for Data Analytics",
    # long_description=long_description,
    install_requires=[
        "psycopg2-binary",
        "pyodbc",
        "pandas",
        "SQLAlchemy",
        # "streamlit"
    ],
    python_requires=">=3.6",
    packages=find_packages()
)
