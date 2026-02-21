import dlt
import dlt
import sys
import os
import logging
import json
from dlt.common.pipeline import get_dlt_pipelines_dir
logger = logging.getLogger(__name__)
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from ingestion.sources.carvago import carvago_source
from ingestion.config.base_config import get_pg_credentials
import logging

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from project_files import functions
from ingestion.sources.carvago import carvago_source
from ingestion.config.base_config import run_dlt_pipeline

destination=dlt.destinations.postgres(credentials=get_pg_credentials())
DATASOURCE = 'CARVAGO'
PIPELINE_NAME = 'carvago_to_postgresql'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
PIPELINE_RUN_PARAMETERS = json.loads(
    config_dictionary['RUN_CONFIGS']
    )

pipeline = run_dlt_pipeline(
    pipeline_name=PIPELINE_NAME,
    source_func=lambda **p: carvago_source(job_id=1,  **p),
    run_parameters=PIPELINE_RUN_PARAMETERS,
    destination=destination,
    dataset_name=DATASOURCE,
    export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
    log_dir=config_dictionary['DLT_PIPELINE_LOGS_DIR'],
    write_disposition = 'merge'
    # write disposition docs: https://dlthub.com/docs/general-usage/incremental-loading
)

if __name__ == "__main__":
    pipeline
