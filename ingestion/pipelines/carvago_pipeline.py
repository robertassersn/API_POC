import dlt
import dlt
import sys
import os
import logging

logger = logging.getLogger(__name__)
dlt.config["runtime.log_level"] = "WARNING"
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from ingestion.sources.carvago import carvago_source
from ingestion.config.base_config import get_pg_credentials
import logging
logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="carvago",
        export_schema_path="schemas/export",
        destination=dlt.destinations.postgres(credentials=get_pg_credentials()),
        dataset_name="carvago",
    )

    load_info = pipeline.run(
        carvago_source(
            # country=32,
            make="MAKE_LAND_ROVER"
            # mileage_from=2500,
            # mileage_to=200000,
            # power_from=25,
            # power_to=296,
            # price_from=4000,
            # registration_date_from=2006,
            # registration_date_to=2026,
        )
    )