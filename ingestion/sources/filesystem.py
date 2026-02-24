import dlt
import os
import sys
import requests
import xmltodict
from typing import Iterator, Optional
from datetime import datetime
import json
from pathlib import Path
import gzip

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from project_files import functions

os.environ["RUNTIME__LOG_LEVEL"] = "INFO"
# DATASOURCE = 'FILESYSTEM'
# config_dictionary = functions.read_config_segment(segment=DATASOURCE)

@dlt.source
def filesystem_source(
    job_id: str,
    config_dictionary: str,
    indicator: str,
    country: str,
):
    @dlt.resource(name=f"{country}_{indicator}", write_disposition="merge", primary_key="indicator_country_date")
    def filesystem_worldbank_resource() -> Iterator[dict]:
        raw_dir = Path(config_dictionary['DIR_DOWNLOADED_FILES'],f'{country}_{indicator}')
        
        for gz_file in raw_dir.glob(f"**/*.jsonl.gz"):
            
            print('GZ FILES',gz_file, raw_dir)
            with gzip.open(gz_file, "rt") as f:
                for line in f:
                    row = json.loads(line)
                    # remove dlt metadata fields before load
                    row = {k: v for k, v in row.items() if not k.startswith("_dlt_")}
                    yield row
    return filesystem_worldbank_resource