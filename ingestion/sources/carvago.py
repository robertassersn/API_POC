import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


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


import dlt
import requests
from typing import Iterator
from typing import Optional, Union, List


@dlt.source
def carvago_source(
    job_id: str,
    # Optional params - default None
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
):
    @dlt.resource(name="listedcars", write_disposition="merge", primary_key="id")
    def listedcars_resource() -> Iterator[dict]:
        base_url = "https://api.carvago.com/api/listedcars"
        offset = 0

        while True:
            # Start with required params only
            params = {
                "power-unit": "kw",
                "sort": "publish-date",
                "direction": "desc",
                "limit": page_size,
                "offset": offset,
            }

            # Add optional params only if provided
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
            for record in data:
                record["job_id"] = job_id  # inject here
                yield record
            if not data:
                break

            yield from data

            if len(data) < page_size:
                break

            offset += page_size

    return listedcars_resource