"""Global configuration helpers."""
import os
import sys
import logging
import dlt
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from datetime import datetime
from functools import wraps
from project_files import functions
import tenacity
from tenacity import wait_fixed,retry,stop_after_attempt
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


def with_logging_dlt(
    pipeline_name: str,
    log_dir: str = "logs",
    level: int = logging.DEBUG
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            file_handler = logging.FileHandler(
                f"{log_dir}/{pipeline_name}_{timestamp}.log"
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(
                "%(asctime)s|[%(levelname)s]|%(name)s|%(filename)s|%(funcName)s:%(lineno)d|%(message)s"
            ))

            root_logger = logging.getLogger()
            root_logger.setLevel(level)
            root_logger.addHandler(file_handler)

            for name in logging.root.manager.loggerDict:
                if name.startswith("dlt"):
                    l = logging.getLogger(name)
                    l.setLevel(level)
                    l.propagate = True
                    l.addHandler(file_handler)

            try:
                return func(*args, **kwargs)
            finally:
                file_handler.close()
                root_logger.removeHandler(file_handler)
                for name in logging.root.manager.loggerDict:
                    if name.startswith("dlt"):
                        logging.getLogger(name).removeHandler(file_handler)

        return wrapper
    return decorator

def run_dlt_pipeline(
    pipeline_name: str,
    source_func,
    destination,
    dataset_name: str,
    export_schema_path: str,
    log_dir: str,
    write_disposition: str = "append",
    run_parameters: list = None,  # now optional
):
    @with_logging_dlt(
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
        
        if run_parameters:
            for iteration_params in run_parameters:
                logger.info(f"Running extraction with params: {iteration_params}")
                load_info = pipeline.run(
                    source_func(**iteration_params),
                    write_disposition=write_disposition,
                )
                logger.info(f"Load info: {load_info}")
        else:
            logger.info("Running extraction with no params")
            load_info = pipeline.run(
                source_func(),
                write_disposition=write_disposition,
            )
            logger.info(f"Load info: {load_info}")

        logger.info("pipeline run complete")

    _run()


import requests

@tenacity.retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(60),
    reraise=True,
)
def requests_get_page(
    base_url
    , params
    , headers = None
    , timeout = 30
    ) -> dict:

    response = requests.get(
        base_url
        , params=params
        , headers = headers
        , timeout=30
    )
    response.raise_for_status()
    return response