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
    
def save_as_csv(data, filepath):
    """Save timeline data as CSV."""
    if not data:
        raise ValueError("No data to save")
    
    # Extract query names from first record
    queries = [item["query"] for item in data[0]["values"]]
    
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow(["date"] + queries)
        
        # Data rows
        for point in data:
            values = {v["query"]: v["extracted_value"] for v in point["values"]}
            row = [point["date"]] + [values.get(q) for q in queries]
            writer.writerow(row)


def save_as_json(data, filepath):
    """Save timeline data as JSON."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def delete_files_from_directory(files_path):
    for item in os.listdir(files_path):
        item_path = os.path.join(files_path, item)
        if os.path.isdir(item_path):
            shutil.rmtree(item_path) # remove subdirectories
        else: 
            os.remove(item_path)

def archive_files(
        files_path
        ,archived_files_path
        ,archived_file_prefix
    ): 
    current_timestamp = get_current_timestamp()
    archived_file_name = f'{archived_files_path}/{archived_file_prefix}_{current_timestamp}'
    shutil.make_archive(archived_file_name,'zip',files_path)

def archive_files_and_cleanup(
        files_path
        ,archived_files_path
        ,archived_file_prefix
    ):
    archive_files(
        files_path
        ,archived_files_path
        ,archived_file_prefix
    )
    delete_files_from_directory(files_path)

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

def load_execution_plan(job_name, data_source,connection):
    """
    Retrieve the job configuration from the Vertica database.

    :param connection: Active Vertica database connection
    :param schema: Schema where the configuration table exists
    :param job_name: Name of the job
    :param job_step: Job step name
    :param data_source: Job parameter (e.g., SNC_FR)
    :return: Execution plan as a list of dictionaries
    """
    cursor = connection.cursor()
    cursor.execute(select_sql, [job_name, data_source])
    rows = cursor.fetchall()

    execution_plan = []

    # Reconstruct the execution plan from the flat rows
    for row in rows:
        job_step_order,step_name, step_type, target_table, disabled, source,destination,data_source = row

        # Convert input_models back to a list

        # Rebuild the task dictionary
        task = {
            'step_name': row[step_name]
            ,'job_step_order':row[job_step_order]
            ,'step_type': row[step_type]
            ,'target_table': row[target_table]
            ,'disabled': row[disabled]
            ,'source':row[source]
            ,'destination':row[destination]
            ,'data_source':data_source
        }
        execution_plan.append(task)
    return execution_plan

def string_replace_parameters(input_string,params):
    for placeholder, value in params.items():
        input_string = input_string.replace(placeholder, value)
    return input_string

def sql_replace_parameters(path_to_sql, params):
    sql_path = path_to_sql#+filename+'.sql'
    with open(sql_path) as sql_file:
        executable_sql = sql_file.read()
        executable_sql = string_replace_parameters( executable_sql,params)
    return executable_sql

def get_sql_operation_type(command: str) -> str:
    # Define regex pattern with word boundaries
    for keyword in ['insert', 'update', 'delete']:
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, command, re.IGNORECASE):
            return keyword.upper()
    return "OTHER"

def sql_function(
        path_to_sql
        ,params
        ,connection_type
        ):
    executable_sql = sql_replace_parameters(path_to_sql, params)
    sqlCommands = executable_sql.split(';')
    # conn_info = get_db_connection_params('vertica_dwh')
    # conn_info = read_config_segment(segment = connection_type)
    with get_connection(connection_type) as conn:
        with conn.cursor() as cur:
            logger.info(f'STARTED:{path_to_sql}')
            try:
                for command in sqlCommands:
                    cur.execute(command)
                logger.info(f'COMPLETED:{path_to_sql}')
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cur.close()
                conn.close()

def sql_verification(path_to_sql, params, connection_type):
    """
    runs sql query, and if sql returns at least 1 row, returns error
    """
    executable_sql = sql_replace_parameters(path_to_sql, params)
    
    with get_connection(connection_type) as conn:
        with conn.cursor() as cursor:
            cursor.execute(executable_sql)
            result = cursor.fetchone()
    if result is not None:
        message = f'''
            {path_to_sql} VERIFICATION FAILED
            

            {executable_sql}
        '''
        logger.error(message)
        raise(message)
    else:
        logger.info(f"{path_to_sql} VERIFICATION PASSED")
    return result


def list_files_in_directory_regex(directory, regex_pattern, recursive=False):
    """
    List files in directory matching a regex pattern.
    
    Args:
        directory: Directory path to search
        regex_pattern: Regex pattern (e.g., r"data_\d{4}-\d{2}-\d{2}\.csv")
        recursive: If True, search subdirectories
    
    Returns:
        List of file paths
    """
    if not os.path.isdir(directory):
        raise ValueError(f"Directory does not exist: {directory}")
    
    compiled_pattern = re.compile(regex_pattern)
    matched_files = []
    
    if recursive:
        for root, dirs, files in os.walk(directory):
            for filename in files:
                if compiled_pattern.search(filename):
                    matched_files.append(os.path.join(root, filename))
    else:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath) and compiled_pattern.search(filename):
                matched_files.append(filepath)
    
    return sorted(matched_files)

def load_parquet_to_postgres(filepath, table_name, connection_type, truncate=False):
    """
    Load parquet file into PostgreSQL using COPY.
    
    Args:
        filepath: Path to parquet file
        table_name: Destination table name with schema (e.g., 'staging.google_trends')
        connection_type: Connection type for get_connection()
        truncate: If True, truncate table before loading
    
    Returns:
        Number of rows loaded
    """
    table = pq.read_table(filepath)
    
    buffer = StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    
    for batch in table.to_batches():
        rows = zip(*[col.to_pylist() for col in batch.columns])
        writer.writerows(rows)
    
    buffer.seek(0)
    
    with get_connection(connection_type) as conn:
        with conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {table_name}")
            
            cur.copy_expert(
                f"COPY {table_name} ({','.join(table.column_names)}) FROM STDIN WITH CSV",
                buffer
            )
        conn.commit()
    
    logger.info(f"Loaded {table.num_rows} rows into {table_name}")
    return table.num_rows

def load_files_from_directory_to_postgres(
        file_directory
        ,filename_pattern
        ,connection_type 
        ,target_table
    ):
    matching_files = list_files_in_directory_regex(
        directory = file_directory
        , regex_pattern = f'{filename_pattern}'
        , recursive=False
    )
    for filename in matching_files:
        load_parquet_to_postgres(
            filepath = filename 
            , table_name = target_table
            , connection_type = connection_type
            , truncate=False
            )
        
def start_log(in_file):
    # Create "Logs" directory if it doesn't exist
    current_directory = os.path.dirname(os.path.abspath(sys.argv[0]))  # Get the root script directory
    log_dir = os.path.join(current_directory, "Logs")  # Ensure the "Logs" directory is in the root script's directory
    os.makedirs(log_dir, exist_ok=True)
    # Generate log file name with timestamp and store it in "Logs" directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
    logfile_name = os.path.join(log_dir, f"{os.path.splitext(os.path.basename(in_file))[0]}_{timestamp}.log")  
    logging.basicConfig(level = logging.DEBUG,format = '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',filename=logfile_name,filemode='w')
    console = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)
    logger = logging.getLogger(__name__)
    logger.info(f'STARTED:{in_file}')

def end_log():
    logging.info(f'JOB COMPLETED')

def end_log_error():
    logging.info(f'JOB FINISHED WITH ERROR')


def truncate_table(target_table: str, connection_type: str) -> bool:
    """
    Truncate a PostgreSQL table.
    
    Args:
        target_table: Schema-qualified table name (e.g., 'schema_name.table_name')
        connection_type: Connection type for get_connection()
    
    Returns:
        True if successful
    """
    logger.info(f'TRUNCATING TABLE {target_table}')
    try:
        with get_connection(connection_type) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE {target_table};')
            conn.commit()
        return True
    except Exception as e:
        error_message = f'truncate_table failed for {target_table}: {e}'
        logger.error(error_message)
        raise Exception(error_message) 

def get_job_run_id(connection_type: str) -> int:
    """
    Get next job_run_id from sequence.
    
    Args:
        connection_type: Connection type for get_connection()
    
    Returns:
        Next job_run_id value
    """
    try:
        with get_connection(connection_type) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT nextval('job_info.seq_job_run_id')")
                job_run_id = cursor.fetchone()[0]
        return job_run_id
    except Exception as e:
        raise Exception(f'get_job_run_id failed: {e}') from e
    

def start_job_run(connection_type,job_run_id):
    with get_connection(connection_type) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT nextval('job_info.seq_job_run_id')")


def save_raw_json(data: dict, keyword: str,raw_json_dir : str) -> str:
    """Save raw API response to JSON file."""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"google_trends_{keyword}_{timestamp}.json"
    filepath = os.path.join(raw_json_dir, filename)
    
    print(f"Saving to: {filepath}")  # Debug
    print(f"Directory exists: {os.path.exists(raw_json_dir)}")  # Debug
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"File saved: {os.path.exists(filepath)}")  # Debug
    return filepath