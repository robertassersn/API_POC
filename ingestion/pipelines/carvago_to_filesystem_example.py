import dlt
import dlt
import sys
import os
import logging

# dlt.config["runtime.log_level"] = "WARNING"
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from project_files import functions
from ingestion.sources.carvago import carvago_source
import logging
from datetime import datetime

config_dictionary = functions.read_config_segment()
destination_raw = dlt.destinations.filesystem(bucket_url=config_dictionary['CARVAGO_DIR_DOWNLOADED_FILES'])
os.environ["RUNTIME__LOG_LEVEL"] = "INFO"  # set before pipeline creation

@functions.with_logging(log_dir="ingestion/pipelines/logs", level=logging.INFO)
def run_pipeline():
    logger = logging.getLogger(__name__) 
    dlt_logger = logging.getLogger("dlt")
    dlt_logger.propagate = True
    dlt_logger.setLevel(logging.INFO)    
    # Copy handlers from root logger to dlt logger if propagation alone doesn't work
    for handler in logging.getLogger().handlers:
        dlt_logger.addHandler(handler)
    logger.info("Starting pipeline run")
    pipeline = dlt.pipeline(
        pipeline_name="carvago_to_filesystem",
        export_schema_path="ingestion/schemas/export",
        destination=destination_raw,
        dataset_name="carvago"
    )
    logger.info("Pipeline defined, starting data extraction")
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
    logger.info("Pipeline run complete")


if __name__ == "__main__":
    run_pipeline()