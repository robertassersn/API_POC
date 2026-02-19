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

def cleanup_old_logs(log_dir, days_to_keep=10):
    cutoff_time = datetime.now() - timedelta(days=days_to_keep)
    
    for filename in os.listdir(log_dir):
        file_path = os.path.join(log_dir, filename)
        
        if filename.endswith(".log"):  # Ensure we're only handling log files
            file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
            
            if file_creation_time < cutoff_time:
                os.remove(file_path)  # Delete old log file
                logging.info(f"Deleted old log file: {filename}")

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


def save_output_to_file(data, directory, filename, file_type):
    """
    Save data to file in specified format.
    
    Args:
        data: List of timeline data from API
        directory: Directory path to save file
        filename: Name of file (without extension)
        file_type: 'csv' or 'json'
    
    Returns:
        Full path to saved file
    """
    os.makedirs(directory, exist_ok=True)
    file_type = file_type.lower()
    filepath = os.path.join(directory, f"{filename}.{file_type}")

    # Use lambdas to defer execution
    FILE_TYPES = {
        "csv": lambda: save_as_csv(data, filepath),
        "json": lambda: save_as_json(data, filepath)
    }
    
    try:
        if file_type not in FILE_TYPES:
            raise ValueError(f"Unsupported file type: {file_type}. Use 'csv' or 'json'.")
        FILE_TYPES[file_type]()
        return filepath
    except Exception as e:
        raise RuntimeError(f"save_output_to_file function failed:\n ERROR: {e}")
    

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

