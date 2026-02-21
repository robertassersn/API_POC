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
import logging
from datetime import datetime
os.environ["RUNTIME__LOG_LEVEL"] = "INFO"  # set before pipeline creation
DATASOURCE = 'CARVAGO'
PIPELINE_NAME = 'carvago_to_filesystem'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
PIPELINE_RUN_PARAMETERS = json.loads(
    config_dictionary['RUN_CONFIGS']
    )

destination_raw = dlt.destinations.filesystem(bucket_url=config_dictionary['DIR_DOWNLOADED_FILES'])

@functions.with_logging(
        log_dir=config_dictionary['DLT_PIPELINE_LOGS_DIR']
        ,pipeline_name = PIPELINE_NAME
        , level=logging.INFO
    )
def run_pipeline():
    logger = logging.getLogger(__name__) 
    dlt_logger = logging.getLogger("dlt")
    dlt_logger.propagate = True
    dlt_logger.setLevel(logging.INFO)    
    # Copy handlers from root logger to dlt logger if propagation alone doesn't work
    for handler in logging.getLogger().handlers:
        dlt_logger.addHandler(handler)
    logger.info("starting pipeline run" )
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
        destination=destination_raw,
        dataset_name=DATASOURCE
    )
    logger.info("starting data extraction")
    for ITERATION_PARAMETERS in PIPELINE_RUN_PARAMETERS:
        logger.info(f"Running extraction with params: {ITERATION_PARAMETERS}")
        load_info = pipeline.run(
            carvago_source(job_id= 1 ,**ITERATION_PARAMETERS),
            write_disposition="append"
        )
        logger.info(f"Load info: {load_info}")

    logger.info("pipeline run complete")


if __name__ == "__main__":
    run_pipeline()