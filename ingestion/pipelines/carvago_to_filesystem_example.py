import dlt
import dlt
import sys
import os
import logging
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
destination_raw = dlt.destinations.filesystem(bucket_url=config_dictionary['DIR_DOWNLOADED_FILES'])

@functions.with_logging(
        log_dir=config_dictionary['PIPELINE_LOGS_DIR']
        , level=logging.INFO
    )
def run_carvago_pipeline():
    logger = logging.getLogger(__name__) 
    dlt_logger = logging.getLogger("dlt")
    dlt_logger.propagate = True
    dlt_logger.setLevel(logging.INFO)    
    # Copy handlers from root logger to dlt logger if propagation alone doesn't work
    for handler in logging.getLogger().handlers:
        dlt_logger.addHandler(handler)
    logger.info(f"datasource: {DATASOURCE} \n pipeline: {PIPELINE_NAME} \n EVENT: starting pipeline run" )
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        export_schema_path=config_dictionary['DLT_SOURCE_SCHEMA_DIR'],
        destination=destination_raw,
        dataset_name=DATASOURCE
    )
    logger.info("datasource: {DATASOURCE}\n pipeline: {PIPELINE_NAME} \n EVENT: starting data extraction")
    pipeline.run(
        carvago_source(
            job_id=str(1),
            # country=32,
            make="MAKE_LAND_ROVER"
            # mileage_from=2500,
            # mileage_to=200000,
            # power_from=25,
            # power_to=296,
            # price_from=4000,
            # registration_date_from=2006,
            # registration_date_to=2026
        )
        , write_disposition="replace"
    )
    logger.info("datasource: {DATASOURCE} \n pipeline: {PIPELINE_NAME} \n EVENT: pipeline run complete")


if __name__ == "__main__":
    run_carvago_pipeline()