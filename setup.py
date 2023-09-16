# coding : utf-8

from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="db_analytics_tools",
    version="0.1.4.7",
    url="https://josephkonka.com/#projects",
    download_url="https://github.com/joekakone/db-analytics-tools",
    project_urls={
        "Bug Tracker": "https://github.com/joekakone/db-analytics-tools/issues",
        "Documentation": "https://joekakone.github.io/db-analytics-tools-docs",
        "Source Code": "https://github.com/joekakone/db-analytics-tools",
    },
    license="MIT",
    author="Joseph Konka",
    author_email="contact@josephkonka.com",
    description="Databases Tools for Data Analytics",
    keywords="databases analytics etl sql orc",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "psycopg2-binary",
        "pyodbc",
        "pandas",
        "SQLAlchemy",
        "streamlit"
    ],
    python_requires=">=3.6",
    packages=find_packages()
)
