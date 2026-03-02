from dotenv import load_dotenv
load_dotenv()
import os 
import sys
import configparser
import logging
from datetime import date, timedelta,datetime
from dateutil.relativedelta import relativedelta
import pyarrow.parquet as pq
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
