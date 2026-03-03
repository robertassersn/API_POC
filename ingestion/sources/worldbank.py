import dlt
import os
import sys
import xmltodict
from typing import Iterator
from datetime import datetime
from pathlib import Path

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from project_files import functions
from ingestion.ingestion_functions.general_functions import requests_get_page

os.environ["RUNTIME__LOG_LEVEL"] = "INFO"
DATASOURCE = 'WORLD_BANK'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)

# https://api.worldbank.org/v2/country/US/indicator/NY.GDP.MKTP.CD?format=xml
@dlt.source
def worldbank_source(
    job_id: str,
    indicator: str,
    country: str,
    output_format: str = "xml",
    per_page: int = 1000,
):
    @dlt.resource(name=f"{country}_{indicator}", write_disposition="replace", primary_key="indicator_country_date")
    def worldbank_resource() -> Iterator[dict]:
        raw_dir = Path(config_dictionary['DIR_RAW_FILES'])
        raw_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        page = 1
        total_yielded = 0

        while True:
            base_url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
            params = {
                "format": output_format,
                "per_page": per_page,
                "page": page,
            }

            # response = requests.get(url, params=params, timeout=60)
            # response.raise_for_status()  
            response = requests_get_page(
                    base_url = base_url
                    ,params = params 
                )              
            raw_file = raw_dir / f"worldbank_{country}_{indicator}_{timestamp}_page{page}.xml"
            with open(raw_file, "wb") as f:
                f.write(response.content)

            parsed = xmltodict.parse(response.content)
            root = parsed.get("wb:data", {})

            records = root.get("wb:data", [])
            if isinstance(records, dict):
                records = [records]

            if not records:
                break

            for record in records:
                row = {}
                for key, value in record.items():
                    clean_key = key.replace("wb:", "")
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            clean_subkey = subkey.replace("@", "").replace("#", "text_")
                            row[f"{clean_key}_{clean_subkey}"] = subvalue
                    else:
                        row[clean_key] = value

                row["job_id"] = job_id
                row["indicator_country_date"] = f"{row.get('indicator_id')}_{row.get('country_id')}_{row.get('date')}"

                yield row
                total_yielded += 1

            total_pages = int(root.get("@pages", 1))
            if page >= total_pages:
                break

            page += 1

    return worldbank_resource
