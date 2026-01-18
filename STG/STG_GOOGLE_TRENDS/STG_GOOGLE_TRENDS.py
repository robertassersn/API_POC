'''
parameters required for job to run, 
quite few of provided parameters below could be taken automatically by python functions
Many of hardcored values may be stored inside of database and enabled and disabled as steps or taken automatically from config during job start
''' 
import sys 
import os 
import google_trends_functions
base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),'../../'
        )
    )
from project_files import functions
sys.path.append(base_path)

job_config = {
    "JOB_NAME" : "STG_GOOGLE_TRENDS"
    ,"DATA_SOURCE":"GOOGLE"
    ,"CONNECTION_TYPE":"POSTGRESQL_CONN"
    ,"FILE_DIRECTORY":os.path.dirname(os.path.abspath(__file__))
    ,"FILE_NAME":os.path.basename(__file__)
    ,"JOB_LAYER":'STG'
}

path_to_sql = os.path.join(
    job_config['FILE_DIRECTORY']
    ,'sql\\'
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
]

sqls_to_execute = {
    name: f'{path_to_sql}{name}.sql' for name in sql_files
}
# dwh_conn = functions.get_connection(
#         connection_type 
#     )
'''
params CAN and should be automated
'''
params = {
    "${SCHEMA_GOOGLE_TRENDS_TEMP}":config_dictionary['SCHEMA_GOOGLE_TRENDS_TEMP']
    ,"${SCHEMA_GOOGLE_TRENDS_TEMP}":config_dictionary['SCHEMA_GOOGLE_TRENDS_TEMP']
    ,"${SCHEMA_JOB_INFO}":config_dictionary['SCHEMA_JOB_INFO']
    ,"${JOB_STATUS_STARTED}":config_dictionary['JOB_STATUS_STARTED']
    ,"${JOB_STATUS_FINISHED}":config_dictionary['JOB_STATUS_FINISHED']
    ,"${JOB_RUN_ID}":str(
        functions.get_job_run_id(connection_type = job_config['CONNECTION_TYPE'])
    )
}
print(f'PARAMETER LIST: \n {params}')

"""
ALGORITHM OF THE JOB
"""
functions.sql_function(sqls_to_execute["start_job_run"] ,params ,connection_type)

# print(path_to_sql)