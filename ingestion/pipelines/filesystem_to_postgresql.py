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
from ingestion.config.base_config import get_pg_credentials
from project_files import functions
from ingestion.config.base_config import run_dlt_pipeline

destination=dlt.destinations.postgres(credentials=get_pg_credentials())
DATASOURCE = 'FILESYSTEM'
PIPELINE_NAME = 'WORLDBANK_FROM_FILESYSTEM_TO_POSTGRESQL'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
PIPELINE_RUN_PARAMETERS = json.loads(
    config_dictionary['RUN_CONFIGS']
    )

pipeline = run_dlt_pipeline(
    pipeline_name=PIPELINE_NAME,
    source_func=lambda **p: filesystem_source(job_id=1,config_dictionary=config_dictionary,  **p),
    run_parameters=PIPELINE_RUN_PARAMETERS,
    destination=destination,
    dataset_name='WORLD_BANK',
    export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
    log_dir=config_dictionary['DLT_PIPELINE_LOGS_DIR'],
    write_disposition = 'merge'
    # write disposition docs: https://dlthub.com/docs/general-usage/incremental-loading
)

if __name__ == "__main__":
    pipeline
