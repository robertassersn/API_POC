import dlt
import dlt
import sys
import os
import logging
import json
logger = logging.getLogger(__name__)
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from ingestion.sources.filesystem import filesystem_source
from project_files import functions
from ingestion.ingestion_functions.general_functions import run_dlt_pipeline,get_pg_credentials

destination=dlt.destinations.postgres(credentials=get_pg_credentials())
DATASOURCE = 'FILESYSTEM'
PIPELINE_NAME = 'WORLDBANK_FROM_FILESYSTEM_TO_POSTGRESQL'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
PIPELINE_RUN_PARAMETERS = json.loads(
    config_dictionary['RUN_CONFIGS']
    )

pipeline = run_dlt_pipeline(
    pipeline_name=PIPELINE_NAME,
    source_func=lambda **p: filesystem_source(job_id=2,config_dictionary=config_dictionary,  **p),
    run_parameters=PIPELINE_RUN_PARAMETERS,
    destination=destination,
    dataset_name='WORLD_BANK'.lower(),
    export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
    log_dir=config_dictionary['DLT_PIPELINE_LOGS_DIR'],
    write_disposition = 'replace'
    # write disposition docs: https://dlthub.com/docs/general-usage/incremental-loading
)

if __name__ == "__main__":
    pipeline
