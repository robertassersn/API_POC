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
from ingestion.ingestion_functions.general_functions import requests_get_page
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
DATASOURCE = 'GITHUB'
config_dictionary = functions.read_config_segment(segment=DATASOURCE)
github_token = config_dictionary['API_TOKEN']
headers = {"Authorization": f"Bearer {github_token}"}

@dlt.source
def github_source(
    job_run_id: str,
    repository: str,
    state: str,
    per_page: int = 100,
    max_records: int = None,
):
    
    @dlt.resource(name="issues", write_disposition="merge", primary_key="id")
    def issues(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental("updated_at", initial_value="2026-01-01T00:00:00Z")
    ) -> Iterator[dict]:
        base_url = f"https://api.github.com/repos/{repository}/issues"
        page = 1
        total_yielded = 0

        raw_dir = Path(config_dictionary['DIR_RAW_FILES'])
        raw_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = raw_dir / f"github_issues_vscode_{timestamp}.jsonl"

        with open(raw_file, "w") as f:
            while True:
                effective_page_size = int(per_page)
                if max_records is not None:
                    remaining = max_records - total_yielded
                    if remaining <= 0:
                        break
                    effective_page_size = min(per_page, remaining)

                params = {
                    "since": updated_at.last_value,
                    "per_page": per_page,
                    "state": state,
                    "page": page,
                }

                response = requests_get_page(
                    base_url = base_url
                    ,params = params 
                    ,headers = headers
                )
                data = response.json()

                if not data:
                    break

                for record in data:
                    record["job_run_id"] = job_run_id
                    f.write(json.dumps(record) + "\n")  # raw nested JSON
                    yield record  # dlt normalizes this
                    total_yielded += 1

                if len(data) < effective_page_size:
                    break

                page += 1

    return issues