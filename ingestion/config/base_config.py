"""Global configuration helpers."""
import os
import sys
import logging
import dlt
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)

from project_files import functions

_conn_cache = {}


def get_conn_info(segment: str = 'POSTGRESQL_CONN') -> dict:
    """Get connection info (cached)."""
    if segment not in _conn_cache:
        _conn_cache[segment] = functions.read_config_segment(segment=segment)
    return _conn_cache[segment]


def get_pg_credentials(segment: str = 'POSTGRESQL_CONN') -> str:
    """Build PostgreSQL connection string."""
    conn = get_conn_info(segment)
    return f"postgresql://{conn['USERNAME']}:{conn['PASSWORD']}@{conn['HOST_NAME']}:{conn['PORT_NUMBER']}/{conn['DATABASE']}"



def run_dlt_pipeline(
    pipeline_name: str,
    source_func,
    run_parameters: list,
    destination,
    dataset_name: str,
    export_schema_path: str,
    log_dir: str,
    write_disposition: str = "append",
):
    @functions.with_logging(
        log_dir=log_dir,
        pipeline_name=pipeline_name,
        level=logging.INFO
    )
    def _run():
        logger = logging.getLogger(__name__)
        dlt_logger = logging.getLogger("dlt")
        dlt_logger.propagate = True
        dlt_logger.setLevel(logging.INFO)
        for handler in logging.getLogger().handlers:
            dlt_logger.addHandler(handler)

        logger.info("starting pipeline run")
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            export_schema_path=export_schema_path,
            destination=destination,
            dataset_name=dataset_name,
        )

        for iteration_params in run_parameters:
            logger.info(f"Running extraction with params: {iteration_params}")
            load_info = pipeline.run(
                source_func(**iteration_params),
                write_disposition=write_disposition,
            )
            logger.info(f"Load info: {load_info}")

        logger.info("pipeline run complete")

    _run()