import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
import os
import sys
import dlt
import requests
from typing import Iterator
from typing import Optional, Union, List
from datetime import datetime
import json
from pathlib import Path
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

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from project_files import functions


os.environ["RUNTIME__LOG_LEVEL"] = "INFO"  # set before pipeline creation
DATASOURCE = 'CARVAGO'
PIPELINE_NAME = 'carvago_to_filesystem'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)

@dlt.source
def carvago_source(
    job_id: str,
    country: Optional[Union[int, List[int]]] = None,
    make: Optional[Union[str, List[str]]] = None,
    mileage_from: Optional[int] = None,
    mileage_to: Optional[int] = None,
    power_from: Optional[int] = None,
    power_to: Optional[int] = None,
    price_from: Optional[int] = None,
    price_to: Optional[int] = None,
    registration_date_from: Optional[int] = None,
    registration_date_to: Optional[int] = None,
    page_size: int = 100,
    max_records: Optional[int] = None,
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

                response = requests.get(base_url, params=params, timeout=30)
                response.raise_for_status()
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

# @dlt.source
# def carvago_source(
#     job_id: str,
#     country: Optional[Union[int, List[int]]] = None,
#     make: Optional[Union[str, List[str]]] = None,
#     mileage_from: Optional[int] = None,
#     mileage_to: Optional[int] = None,
#     power_from: Optional[int] = None,
#     power_to: Optional[int] = None,
#     price_from: Optional[int] = None,
#     price_to: Optional[int] = None,
#     registration_date_from: Optional[int] = None,
#     registration_date_to: Optional[int] = None,
#     page_size: int = 100,
#     max_records: Optional[int] = None,  # None = no limit
# ):
#     @dlt.resource(name="listedcars", write_disposition="merge", primary_key="id")
#     def listedcars_resource() -> Iterator[dict]:
#         base_url = "https://api.carvago.com/api/listedcars"
#         offset = 0
#         total_yielded = 0

#         while True:
#             # Shrink page size on last page if max_records is set
#             effective_page_size = page_size
#             if max_records is not None:
#                 remaining = max_records - total_yielded
#                 if remaining <= 0:
#                     break
#                 effective_page_size = min(page_size, remaining)

#             params = {
#                 "power-unit": "kw",
#                 "sort": "publish-date",
#                 "direction": "desc",
#                 "limit": effective_page_size,
#                 "offset": offset,
#             }

#             if country is not None:
#                 params["country[]"] = country
#             if make is not None:
#                 params["make[]"] = make
#             if mileage_from is not None:
#                 params["mileage-from"] = mileage_from
#             if mileage_to is not None:
#                 params["mileage-to"] = mileage_to
#             if power_from is not None:
#                 params["power-from"] = power_from
#             if power_to is not None:
#                 params["power-to"] = power_to
#             if price_from is not None:
#                 params["price-from"] = price_from
#             if price_to is not None:
#                 params["price-to"] = price_to
#             if registration_date_from is not None:
#                 params["registration-date-from"] = registration_date_from
#             if registration_date_to is not None:
#                 params["registration-date-to"] = registration_date_to

#             response = requests.get(base_url, params=params, timeout=30)
#             response.raise_for_status()
#             data = response.json()

#             if not data:
#                 break

#             for record in data:
#                 record["job_id"] = job_id
#                 yield record
#                 total_yielded += 1

#             if len(data) < effective_page_size:
#                 break

#             offset += page_size

#     return listedcars_resource