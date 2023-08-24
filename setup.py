# from distutils.core import setup
from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

# print(find_packages())
setup(
    name='db_analytics_tools',
    version='0.1.2',
    # packages=['db_analytics_tools'],
    url='http://josephkonka.com/',
    download_url="https://github.com/joekakone/db-analytics-tools",
    license='MIT',
    author='Joseph Konka',
    author_email='contact@josephkonka.com',
    description='Databases Tools for Data Analytics',
    keywords='databases analytics etl sql orc',
    long_description='Databases Tools for Data Analytics',
    # long_description=long_description,
    install_requires=[
        'psycopg2-binary',
        'pandas',
    ],
    python_requires='>=3.6',
    packages=find_packages()
)
