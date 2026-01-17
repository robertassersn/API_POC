from dotenv import load_dotenv
load_dotenv()
import os 
import sys
import configparser
import logging
import psycopg2
from psycopg2 import OperationalError
from datetime import date, timedelta,datetime
from dateutil.relativedelta import relativedelta
import re 
import shutil
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

def get_connection(
        connection_type
    ):

    conn_info = read_config_segment(segment = connection_type)
    db_type = conn_info.get('type')
    CONNECTORS = {
        'POSTGRESQL':lambda:get_postgresql_connection(conn_info)
    }

    return CONNECTORS[f'{connection_type}']()

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

def sql_function(path_to_sql,filename,params,JOB_NAME,DATA_SOURCE,vertica_conn,job_run_id,execution_step):
    executable_sql = sql_replace_parameters(path_to_sql, params)
    sqlCommands = executable_sql.split(';')
    reference_link=execution_step['reference_link']
    msg_type=execution_step['msg_type']
    step_name = execution_step['step_name']
    audit_log_flag = execution_step['audit_log_flag']
    conn_info = get_db_connection_params('vertica_dwh')
    with get_connection(conn_info) as conn:
        with conn.cursor() as cur:
            logger.info(f'STARTED:{path_to_sql}')
            cur.execute("SET SESSION AUTOCOMMIT TO OFF;")
            try:
                for command in sqlCommands:
                    cur.execute(command)
                    row_count = cur.fetchall()  # This helps surface constraint violations
                    operation_type = get_sql_operation_type(command)
                    if operation_type != 'OTHER':
                        logger.info(f'{operation_type} statement affected row_count: {row_count}')
                logger.info(f'COMPLETED:{path_to_sql}')
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cur.close()
                conn.close()

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

def execute_step(job, vertica_conn, path_to_sql_scripts, execution_step, params):
    connection_type = execution_step['connection_type']
    data_source = execution_step['data_source']
    step_type = execution_step['step_type']
    step_name = execution_step['step_name']
    sql_file_name = step_name + '.sql'
    step_path_to_sql_script = os.path.join(path_to_sql_scripts, sql_file_name)

    logger.info(f'Execute step: {step_type} -> {step_name} -> {step_path_to_sql_script}')
    if execution_step.get('disabled') == 'Y':
        logger.warning(f'Step marked as not Valid: {step_type} -> {step_name} -> {step_path_to_sql_script}')
        return

    if step_type == 'T':
        step_destination_table = execution_step['target_table']
        if connection_type == 'vertica': #check_table_lock(vertica_conn,lock_check_schema,lock_check_table):
            sql_function(path_to_sql_scripts,name,params,job.job_name,job.data_source,vertica_conn,job.run_id,execution_step)
        execute_transformation(job, vertica_conn, step_path_to_sql_script, params, step_name,
                               step_destination_table, execution_step, connection_type, data_source)
    elif step_type == 'V':
        step_path_to_message_sql = os.path.join(path_to_sql_scripts, 'message_' + sql_file_name)
        verification_abort = execution_step['msg_type'].upper() == "ERROR"
        execute_verification(job, vertica_conn, step_path_to_sql_script, params, step_name,
                             verification_abort, step_path_to_message_sql, execution_step)

def execute_job_plan(job, vertica_conn, path_to_sql_scripts, execution_plan, params):
    logger.debug('Execute plan execution')
    # test_job_external_database_connection(
    #     vertica_conn = vertica_conn
    #     ,job = job
    #     )
    # Group steps by job_step_order
    grouped_steps = defaultdict(list)
    for step in execution_plan:
        grouped_steps[step['job_step_order']].append(step)

    # Sort by job_step_order to maintain execution order
    for step_order in sorted(grouped_steps.keys()):
        steps = grouped_steps[step_order]
        logger.info(f'Executing steps with job_step_order = {step_order}')
        logger_msg = 'Executing steps '+ str([step['step_name'] for step in steps])
        logger.info(logger_msg)


        # Use ThreadPoolExecutor to run steps in parallel
        with ThreadPoolExecutor() as executor:
            futures = []
            for execution_step in steps:
                futures.append(executor.submit(
                    execute_step,
                    job,
                    vertica_conn,
                    path_to_sql_scripts,
                    execution_step,
                    params
                ))

            # Optionally wait for all to complete
            for future in futures:
                future.result()  # This will raise exceptions if any occurred