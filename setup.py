# from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name='db_analytics_tools',
    version='0.1',
    packages=['db_analytics_tools'],
    url='http://josephkonka.com/',
    license='MIT',
    author='Joseph Konka',
    author_email='contact@josephkonka.com',
    description='Databases Tools for Data Analytics',
    keywords='databases analytics etl sql orc',
    install_requires=[
        'psycopg2-binary',
        'pandas',
    ],
    python_requires='>=3.6'
)
