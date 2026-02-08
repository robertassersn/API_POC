'''
parameters required for job to run, 
quite few of provided parameters below could be taken automatically by python functions
Many of hardcored values may be stored inside of database and enabled and disabled as steps or taken automatically from config during job start
''' 
import sys 
import os 
import google_trends_functions
import logging
logger = logging.getLogger(__name__)
base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__)
        ,'../../'
    )
)
from project_files import functions
cli_passed_args = functions.get_cli_args()
airflow_dag_id = cli_passed_args.get('dag_id')
airflow_dag_run_id = cli_passed_args.get('dag_run_id')
sys.path.append(base_path)

job_config = {
    "JOB_NAME" : "STG_GOOGLE_TRENDS"
    ,"DATA_SOURCE":"GOOGLE"
    ,"CONNECTION_TYPE":"POSTGRESQL_CONN"
    ,"FILE_DIRECTORY":os.path.dirname(os.path.abspath(__file__))
    ,"FILE_NAME":os.path.basename(__file__)
}

path_to_sql = os.path.join(
    job_config['FILE_DIRECTORY']
    ,'sql/'
)
config_dictionary = functions.read_config_segment()
connection_type = 'POSTGRESQL_CONN'
sql_files = [
    'start_job_run'
    ,'validation_trends_search_temp'
    ,'validation_trends_search__values_temp'
    ,'trends_search_insert_to_main'
    ,'trends_search__values_insert_to_main'
    ,'end_job_run'
    ,'end_job_run_error'
]

sqls_to_execute = {
    name: f'{path_to_sql}{name}.sql' for name in sql_files
}
JOB_RUN_ID = str(
        functions.get_job_run_id(connection_type = job_config['CONNECTION_TYPE'])
    )
'''
params CAN and should be automated
'''
params = {
    "${SCHEMA_GOOGLE_TRENDS_TEMP}":config_dictionary['SCHEMA_GOOGLE_TRENDS_TEMP']
    ,"${SCHEMA_GOOGLE_TRENDS}":config_dictionary['SCHEMA_GOOGLE_TRENDS']
    ,"${SCHEMA_JOB_INFO}":config_dictionary['SCHEMA_JOB_INFO']
    ,"${JOB_STATUS_STARTED}":config_dictionary['JOB_STATUS_STARTED']
    ,"${JOB_STATUS_FINISHED}":config_dictionary['JOB_STATUS_FINISHED']
    ,"${JOB_STATUS_ERROR}":config_dictionary['JOB_STATUS_ERROR']
    ,"${JOB_RUN_ID}":JOB_RUN_ID
}

"""
-------------------------------------------------------------------------------------------------------------------------------------------------
ALGORITHM OF THE JOB
-------------------------------------------------------------------------------------------------------------------------------------------------

"""
try:
    functions.start_log(__file__)
    google_trends_functions.insert_into_temp()
    functions.sql_verification(sqls_to_execute["validation_trends_search_temp"] ,params ,connection_type)
    functions.sql_verification(sqls_to_execute["validation_trends_search__values_temp"] ,params ,connection_type)

    functions.sql_function(sqls_to_execute["trends_search_insert_to_main"] ,params ,connection_type)
    functions.sql_function(sqls_to_execute["trends_search__values_insert_to_main"] ,params ,connection_type)
    functions.end_log()
except Exception as e:
    functions.end_log_error()
    # such message template may be more useful for email messages
    # functions.sql_function(sqls_to_execute["end_job_run_error"] ,params ,connection_type)
    error_message = f'''
        JOB_NAME: {job_config['JOB_NAME']}
        DATA_SOURCE: {job_config['DATA_SOURCE']} 
        
        ERROR: {e}
        ----------------------------------------------------
        job can be found here: {job_config['FILE_DIRECTORY']}
    '''
    raise Exception(error_message)
# print(path_to_sql)