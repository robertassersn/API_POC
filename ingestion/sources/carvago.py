import dlt
from dlt.sources.rest_api import rest_api_resources
import os
import sys
import dlt
from typing import Iterator
from datetime import datetime
import json
from pathlib import Path
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from ingestion.config.base_config import requests_get_page
from project_files import functions
# from tenacity import wait_fixed,retry,stop_after_attempt
# https://api.carvago.com/api/listedcars?country[]=32
# &mileage-from=2500
# &mileage-to=200000
# &power-from=25
# &power-to=296
# &power-unit=kw
# &price-from=4000
# &registration-date-from=2006
# &registration-date-to=2026
# &make[]=MAKE_LAND_ROVER&limit=1


os.environ["RUNTIME__LOG_LEVEL"] = "INFO"  # set before pipeline creation
DATASOURCE = 'CARVAGO'
PIPELINE_NAME = 'carvago_to_filesystem'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)

@dlt.source
def carvago_source(
    job_id: str,
    country: str = None,
    make: str = None,
    mileage_from: str = None,
    mileage_to: str =  None,
    power_from: str = None,
    power_to: str = None,
    price_from: str = None,
    price_to: str = None,
    registration_date_from: str = None,
    registration_date_to: str = None,
    page_size: int = 100,
    max_records: str = None,
):
    @dlt.resource(name="listedcars", write_disposition="merge", primary_key="id")
    def listedcars_resource() -> Iterator[dict]:
        base_url = "https://api.carvago.com/api/listedcars"
        offset = 0
        total_yielded = 0

        raw_dir = Path(config_dictionary['DIR_RAW_FILES'])
        raw_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        make_label = make if isinstance(make, str) else (make[0] if isinstance(make, list) else "all")
        raw_file = raw_dir / f"carvago_{make_label}_{timestamp}.jsonl"

        with open(raw_file, "w") as f:
            while True:
                effective_page_size = page_size
                if max_records is not None:
                    remaining = max_records - total_yielded
                    if remaining <= 0:
                        break
                    effective_page_size = min(page_size, remaining)

                params = {
                    "power-unit": "kw",
                    "sort": "publish-date",
                    "direction": "desc",
                    "limit": effective_page_size,
                    "offset": offset,
                }

                if country is not None:
                    params["country[]"] = country
                if make is not None:
                    params["make[]"] = make
                if mileage_from is not None:
                    params["mileage-from"] = mileage_from
                if mileage_to is not None:
                    params["mileage-to"] = mileage_to
                if power_from is not None:
                    params["power-from"] = power_from
                if power_to is not None:
                    params["power-to"] = power_to
                if price_from is not None:
                    params["price-from"] = price_from
                if price_to is not None:
                    params["price-to"] = price_to
                if registration_date_from is not None:
                    params["registration-date-from"] = registration_date_from
                if registration_date_to is not None:
                    params["registration-date-to"] = registration_date_to
                
                response = requests_get_page(
                    base_url = base_url
                    ,params = params 
                )
                data = response.json()

                if not data:
                    break

                for record in data:
                    record["job_id"] = job_id
                    f.write(json.dumps(record) + "\n")  # raw nested JSON
                    yield record  # dlt normalizes this
                    total_yielded += 1

                if len(data) < effective_page_size:
                    break

                offset += page_size

    return listedcars_resource