"""Google Trends pipeline orchestration."""
import dlt
import sys
import os
import logging

logger = logging.getLogger(__name__)

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)

from project_files import functions
from ingestion.sources.google_trends import google_trends_source
from ingestion.config.google_trends_config import get_config
from ingestion.config.base_config import get_pg_credentials


def run_pipeline(keywords: list[str] = None):
    """Run the Google Trends pipeline."""
    config = get_config()
    keywords = keywords or ["vpn", "antivirus", "ad blocker"]
    
    pipeline = dlt.pipeline(
        pipeline_name="google_trends",
        destination=dlt.destinations.postgres(credentials=get_pg_credentials()),
        dataset_name="raw"
    )
    
    load_info = pipeline.run(
        google_trends_source(config=config, keywords=keywords)
    )
    
    logger.info(load_info)
    return load_info


if __name__ == "__main__":
    run_pipeline()