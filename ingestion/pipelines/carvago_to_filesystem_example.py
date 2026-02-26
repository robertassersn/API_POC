import dlt
import dlt
import sys
import os
import logging
import json
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from project_files import functions
from ingestion.sources.carvago import carvago_source
from ingestion.config.base_config import run_dlt_pipeline
import logging
os.environ["RUNTIME__LOG_LEVEL"] = "INFO"  # set before pipeline creation
DATASOURCE = 'CARVAGO'
PIPELINE_NAME = 'carvago_to_filesystem'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
PIPELINE_RUN_PARAMETERS = json.loads(
    config_dictionary['RUN_CONFIGS']
    )
destination = dlt.destinations.filesystem(bucket_url=config_dictionary['DIR_DOWNLOADED_FILES'])

pipeline = run_dlt_pipeline(
    pipeline_name=PIPELINE_NAME,
    source_func=lambda **p: carvago_source(job_id=1, **p),
    run_parameters=PIPELINE_RUN_PARAMETERS,
    destination=destination,
    dataset_name=DATASOURCE.lower(),
    export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
    log_dir=config_dictionary['DLT_PIPELINE_LOGS_DIR'],
)

if __name__ == "__main__":
    pipeline