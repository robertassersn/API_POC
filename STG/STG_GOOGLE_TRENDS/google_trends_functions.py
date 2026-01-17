import sys 
import os 
import serpapi
import time 
import requests

'''
-----------------------
NOTES
2026-01-17
-----------------------
https://github.com/serpapi/serpapi-python
https://serpapi.com/google-trends-api


q limit -> The maximum number of queries per search is 5

'''


# from serpapi import GoogleSearch
base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),'../../'
        )
    )
sys.path.append(base_path)
from common_files import functions,parsing_functions
config_dictionary = functions.read_config_segment(segment = 'GOOGLE_TRENDS')

def construct_date_range(): 
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
            ,"q": config_dictionary['SEARCH_PARAMETER_LIST'] # bad practice to hold this in config, better to pass from DB and have each value in separate row. Have e.g job_parameters table
            ,"date": construct_date_range()
            ,"geo": config_dictionary['COUNTRY']
            ,"api_key": config_dictionary['API_KEY']
            ,"tz": int(config_dictionary['TIMEZONE'])
        }   
    except Exception as e:
        raise(f'construct_search_params function failed:\n ERROR: {e}')
    return params 

def make_api_request(max_retries=3, initial_delay=2):
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
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)  # 2s, 4s, 8s...
                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise RuntimeError(f"API request failed after {max_retries} attempts: {e}")
        
        except Exception as e:
            raise RuntimeError(f"Non-recoverable error: {e}")

def download_data_from_api():
    data = make_api_request()
    return functions.save_output_to_file(
            data = data
            , directory = config_dictionary['GOOGLE_TRENDS_DIR_DOWNLOADED_FILES']
            , filename = 'google_trends_'+str(functions.get_current_timestamp())
            , file_type = 'json'
        )


filename_pattern = 'google_trends.*\\.json'

def parse_downloaded_files():
    converter = parsing_functions.JsonToParquetConverter(
        root_table_name='google_trends'
        ,foreign_key_suffix='_fk'
        ,index_column='_id'
        ,child_separator='__'
    )
    try:
        matching_files = functions.list_files_in_directory_regex(
            directory = config_dictionary['GOOGLE_TRENDS_DIR_DOWNLOADED_FILES']
            , regex_pattern = f'{filename_pattern}'
            , recursive=False
        )

        for file in matching_files:
            converter.convert(
                file
                , config_dictionary['GOOGLE_TRENDS_DIR_PARSED_FILES']
            )
        return True
    except Exception as e:
        raise(f'parse_downloaded_files function failed:\n Error: {e}')
    
    

parse_downloaded_files()