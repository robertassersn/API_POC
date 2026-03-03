import dlt
import sys
import os
import logging
import json
logger = logging.getLogger(__name__)
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from ingestion.sources.google_trends import google_trends_source
from project_files import functions
from ingestion.ingestion_functions.general_functions import run_dlt_pipeline,get_pg_credentials

destination=dlt.destinations.postgres(
    credentials=get_pg_credentials()
    ,staging_dataset_name_layout="%s_temp" 
    )
# ,staging_dataset_name_layout="%s_temp" 
"""
by default library will create google_trends and google_trends_staging schemas
it is possible however to control temporary/staging schema name
e.g have schema google_trends_temp
"""

DATASOURCE = 'GOOGLE_TRENDS'
PIPELINE_NAME = 'google_trends_to_postgresql'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
PIPELINE_RUN_PARAMETERS = json.loads(
    config_dictionary['RUN_CONFIGS']
    )

pipeline = run_dlt_pipeline(
    pipeline_name=PIPELINE_NAME,
    source_func=lambda **p: google_trends_source(
        job_run_id=1
        ,API_KEY= config_dictionary['API_KEY']
        ,  **p
    ),
    run_parameters=PIPELINE_RUN_PARAMETERS,
    destination=destination,
    dataset_name=DATASOURCE.lower(),
    export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
    log_dir=config_dictionary['DLT_PIPELINE_LOGS_DIR'],
    write_disposition = 'merge'
    # write disposition docs: https://dlthub.com/docs/general-usage/incremental-loading
)

if __name__ == "__main__":
    pipeline
