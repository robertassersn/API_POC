import sys 
import os 
import serpapi
import time 
import requests
import logging
logger = logging.getLogger(__name__)
job_config = {
    "JOB_NAME": "STG_GOOGLE_TRENDS"
    ,"DATA_SOURCE": "GOOGLE"
    ,"FILE_DIRECTORY": os.path.dirname(os.path.abspath(__file__))
    ,"FILE_NAME": os.path.basename(__file__)
    ,"JOB_LAYER": 'STG'
}
json_schema_path = os.path.join(job_config['FILE_DIRECTORY'], 'schema.json') 
connection_type = 'POSTGRESQL_CONN'
'''
-----------------------
NOTES
2026-01-17
-----------------------
https://github.com/serpapi/serpapi-python -> generic docs
https://serpapi.com/google-trends-api


q limit -> The maximum number of queries per search is 5

'''

base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),'../../'
        )
    )
sys.path.append(base_path)
from project_files import functions,parsing_functions
config_dictionary = functions.read_config_segment(segment = 'GOOGLE_TRENDS')
# run_timestamp = str(functions.get_current_timestamp())
dwh_conn = functions.get_connection(
        connection_type = 'POSTGRESQL_CONN'
    )

def construct_date_range(): 
    '''
    output format is task specific, therefore for now this function belongs here and not among global files.
    '''
    current_date = functions.get_current_date()
    days_overlap = -int(
        config_dictionary['DAYS_OVERLAP']
    )
    current_date_minus_overlap= functions.date_add(
        current_date
        ,days=days_overlap
    )
    date_range = current_date_minus_overlap + ' ' + current_date
    return date_range

def construct_search_params():
    try:
        params = {
            "engine": "google_trends"
            ,"q": config_dictionary['SEARCH_PARAMETER_LIST'] # bad practice to hold this in config, better to pass from DB and have each value in separate row.
            ,"date": construct_date_range()
            ,"geo": config_dictionary['COUNTRY']
            ,"api_key": config_dictionary['API_KEY']
            ,"tz": int(config_dictionary['TIMEZONE'])
        }   
    except Exception as e:
        raise(f'construct_search_params function failed:\n ERROR: {e}')
    return params 

def make_api_request(max_retries=3, initial_delay=10):
    logger.info('STARTED make_api_request function')
    params = construct_search_params()
    
    retryable_exceptions = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    )
    
    for attempt in range(max_retries):
        try:
            results = serpapi.search(params).as_dict()
            interest_over_time = results.get("interest_over_time", {})
            timeline_data = interest_over_time.get("timeline_data", [])
            return timeline_data
        except retryable_exceptions as e:
            warning_message = f"WARNING, initial data retrieval request failed, retrying:\n ERROR: {e}"
            logger.warning(warning_message)
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)  # 2s, 4s, 8s...
                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                error_message = f"API request failed after {max_retries} attempts: {e}"
                logger.error(error_message)
                raise RuntimeError(error_message)
        
        except Exception as e:
            raise RuntimeError(f"Non-recoverable error: {e}")

def download_data_from_api():
    logger.info('STARTED download_data_from_api function')
    data = make_api_request()
    return functions.save_output_to_file(
            data = data
            , directory = config_dictionary['GOOGLE_TRENDS_DIR_DOWNLOADED_FILES']
            , filename = 'google_trends_'+str(functions.get_current_timestamp())
            , file_type = 'json'
        )


filename_pattern = 'google_trends.*\\.json'

def parse_downloaded_files():
    logger.info('STARTED parse_downloaded_files function')
    
    converter = parsing_functions.JsonToParquetConverter(
        root_table_name='google_trends',
        foreign_key_suffix='_id',
        index_column_suffix='_id',
        child_separator='__',
        key_config={
            'google_trends': ['date', 'timestamp'],
            'google_trends__values': ['query', 'value', 'extracted_value', 'google_trends_id'],
        }
    )
    
    current_step = 'listing files'
    current_file = None
    
    try:
        matching_files = functions.list_files_in_directory_regex(
            directory=config_dictionary['GOOGLE_TRENDS_DIR_DOWNLOADED_FILES'],
            regex_pattern=f'{filename_pattern}',
            recursive=False
        )
        logger.info(f'Files matching search pattern: {matching_files}')
        
        for file in matching_files:
            current_file = file
            
            current_step = 'validating schema'      
            is_valid, error_msg = parsing_functions.validate_json_schema(file, json_schema_path)

            if not is_valid:
                raise ValueError(f"Schema validation failed for {file}: {error_msg}")
                
            current_step = 'parsing file'
            logger.info(f'PARSING FILE: {file}')
            converter.convert(
                file,
                config_dictionary['GOOGLE_TRENDS_DIR_PARSED_FILES'],
                file_suffix=str(functions.get_current_timestamp())
            )
        
        logger.info('COMPLETED parse_downloaded_files function')
        return True
        
    except Exception as e:
        # Detailed log
        logger.error(f"JOB: {job_config['JOB_NAME']} | STEP: {current_step} | FILE: {current_file} | ERROR: {e}")
        
        # Short exception
        raise Exception(f"parse_downloaded_files failed at '{current_step}': {e}") from e
    

# parse_downloaded_files()

timestamp_pattern = r'\d{14}'
def load_parquets_into_temp_table():
    try:
        load_steps = [
            {
                "filename_pattern":f'google_trends_{timestamp_pattern}.parquet'
                ,"target_table":'google_trends_temp.trends_search_temp' # bad practice, its not intuitive that e.g google_trends_20260118083108.parquet loads into following table, in addition schema names HAVE to be parametrized
            }
                                
            ,{
                "filename_pattern":f'google_trends__values_{timestamp_pattern}.parquet'
                ,"target_table":'google_trends_temp.trends_search__values_temp' # same here as above
            }
        ]

        
        for step in load_steps: 
            functions.truncate_table(
                target_table = step['target_table']
                , connection_type = connection_type
            )
            functions.load_files_from_directory_to_postgres(
                file_directory = config_dictionary['GOOGLE_TRENDS_DIR_PARSED_FILES']
                ,filename_pattern = step['filename_pattern']
                ,connection_type = connection_type 
                ,target_table = step['target_table']
            )
    except Exception as e:
        raise Exception(f'load_parquets_into_temp_table function failed: \n ERROR: {e}')
    return True 

def cleanup_files():
    return \
    functions.archive_files_and_cleanup(
        files_path = config_dictionary['GOOGLE_TRENDS_DIR_DOWNLOADED_FILES']
        ,archived_files_path = config_dictionary['GOOGLE_TRENDS_DIR_ARCHIVED_FILES']
        ,archived_file_prefix = 'downloaded_'
    ),\
    functions.archive_files_and_cleanup(
        files_path = config_dictionary['GOOGLE_TRENDS_DIR_PARSED_FILES']
        ,archived_files_path = config_dictionary['GOOGLE_TRENDS_DIR_ARCHIVED_FILES']
        ,archived_file_prefix = 'parsed_'
    )
    

def insert_into_temp():
    # download_data_from_api()
    parse_downloaded_files()
    load_parquets_into_temp_table()
    # cleanup_files()


