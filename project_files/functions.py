from dotenv import load_dotenv
load_dotenv()
import os 
import sys
import configparser
import logging
import psycopg2
from datetime import date, timedelta,datetime
from dateutil.relativedelta import relativedelta
import re 
import shutil
import argparse
import pyarrow.parquet as pq
from io import StringIO
import csv
from contextlib import contextmanager
base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),'../'
        )
    )
sys.path.append(base_path)

_EMAIL_FORMAT_PACKAGE='JOB_NAME'
_EMAIL_FORMAT_PARAMETER='DATA_SOURCE'
_config_parser=None

logger = logging.getLogger(__name__)

def get_current_date():
    return date.today().strftime("%Y-%m-%d")

def get_current_timestamp():
    '''
    Docstring for get_current_timestamp

    output e.g-> 20260117120001
    '''

    return datetime.now().strftime("%Y%m%d%H%M%S")

def date_add(date_str: str, days: int = 0, weeks: int = 0, months: int = 0, years: int = 0) -> str:
    """
    Add or subtract time from a date.
    
    Args:
        date_str: Date in 'YYYY-MM-DD' format
        days: Number of days to add (negative to subtract)
        weeks: Number of weeks to add (negative to subtract)
        months: Number of months to add (negative to subtract)
        years: Number of years to add (negative to subtract)
    
    Returns:
        New date in 'YYYY-MM-DD' format
    """
    d = date.fromisoformat(date_str)
    d += timedelta(days=days, weeks=weeks)
    d += relativedelta(months=months, years=years)
    return d.strftime("%Y-%m-%d")

def get_config_path():
    config_path = os.getenv('MAIN_CONFIG_FILE')
    if config_path:
        logger.info(f"Using config path from environment variable: {config_path}")
        return config_path
    else:
        logger.info(f"os.getenv('MAIN_CONFIG_FILE') no value")
    return config_path

def get_config():
    global _config_parser
    if _config_parser is None:
        #initialise config parser
        config_path=get_config_path()
        _config_parser=configparser.ConfigParser(interpolation=None)
        _config_parser.read(config_path)
        if os.path.isfile(config_path):
            return config_path
        else:
            logger.info('Not existig path:'+config_path)
            raise ValueError(f"Config File doesn't exist: {config_path}")
    return _config_parser

def read_config_segment(segment = 'DEFAULT'): 
    configFilePath = get_config()
    conf_segment_values = _config_parser[segment]
    return conf_segment_values
config_dictionary = read_config_segment()

def get_postgresql_connection(conn_info):
    """
    Create a connection to a PostgreSQL database.

    Args
    conn_info -> connection information provided by config
    
    Returns:
        psycopg2 connection object
    """
    connection = psycopg2.connect(
        host=conn_info["HOST_NAME"],
        port=conn_info["PORT_NUMBER"],
        database=conn_info["DATABASE"],
        user=conn_info["USERNAME"],
        password=conn_info["PASSWORD"]
    )
    
    return connection

@contextmanager
def get_connection(connection_type):
    """
    Get database connection as context manager.
    
    Usage:
        with get_connection('postgresql_dwh') as conn:
            # do stuff
        # connection auto-closes here
    """
    conn_info = read_config_segment(segment=connection_type)
    db_type = conn_info.get('type')
    
    CONNECTORS = {
        'POSTGRESQL': lambda: get_postgresql_connection(conn_info)
    }
    
    conn = CONNECTORS[db_type]()
    try:
        yield conn
    finally:
        conn.close()

import os
import csv
import json
    
def get_cli_args(defaults = None):
    parser = argparse.ArgumentParser()
    args,unknown = parser.parse_known_args()

    arg_dict = {}
    for i in range(0,len(unknown) ,2):
        key = unknown[i].lstrip('-')
        if i + 1 < len(unknown):
            arg_dict[key] = unknown[i + 1]
    if defaults:
        for key, value in defaults.items():
            arg_dict.setdefault(key,value)

    return arg_dict


# import logging
# import os
# from datetime import datetime
# from functools import wraps


# def with_logging(
#     pipeline_name: str,
#     log_dir: str = "logs",
#     level: int = logging.DEBUG
# ):
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             os.makedirs(log_dir, exist_ok=True)
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

#             file_handler = logging.FileHandler(
#                 f"{log_dir}/{pipeline_name}_{timestamp}.log"
#             )
#             file_handler.setLevel(level)
#             file_handler.setFormatter(logging.Formatter(
#                 "%(asctime)s|[%(levelname)s]|%(name)s|%(filename)s|%(funcName)s:%(lineno)d|%(message)s"
#             ))

#             root_logger = logging.getLogger()
#             root_logger.setLevel(level)
#             root_logger.addHandler(file_handler)

#             for name in logging.root.manager.loggerDict:
#                 if name.startswith("dlt"):
#                     l = logging.getLogger(name)
#                     l.setLevel(level)
#                     l.propagate = True
#                     l.addHandler(file_handler)

#             try:
#                 return func(*args, **kwargs)
#             finally:
#                 file_handler.close()
#                 root_logger.removeHandler(file_handler)
#                 for name in logging.root.manager.loggerDict:
#                     if name.startswith("dlt"):
#                         logging.getLogger(name).removeHandler(file_handler)

#         return wrapper
#     return decorator