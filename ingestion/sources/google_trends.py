"""Google Trends dlt source."""
import dlt
import serpapi
import time
import logging
from typing import Iterator
import os,sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)
from ingestion.ingestion_functions.google_trends_functions import construct_date_range

logger = logging.getLogger(__name__)

SCHEMA_CONTRACT = {
    "tables": "freeze",
    "columns": "evolve",
    "data_type": "freeze"
}

def fetch_with_retry(params: dict, max_retries: int = 3, delay: int = 5) -> dict:
    """Fetch from SerpAPI with retry logic."""
    for attempt in range(max_retries):
        try:
            return serpapi.search(params).as_dict()
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise


@dlt.source(schema_contract=SCHEMA_CONTRACT)
def google_trends_source(
        job_run_id: str
        , DAYS_OVERLAP: str
        , COUNTRY: str 
        , API_KEY: str
        , TIMEZONE: str      
        , SEARCH_PARAMETER_LIST: list[str]
    ):
    """Google Trends data source."""
    
    @dlt.resource(write_disposition="merge", primary_key=["keyword", "date"],schema_contract={"tables": "evolve"})
    def trends() -> Iterator[dict]:
        for keyword in [SEARCH_PARAMETER_LIST]:
            params = {
                "engine": "google_trends",
                "q": keyword,
                "date": construct_date_range(DAYS_OVERLAP = DAYS_OVERLAP),
                "geo": COUNTRY,
                "api_key": API_KEY,
                "tz": int(TIMEZONE)
            }
            
            results = fetch_with_retry(params)
                        
            for item in results.get("interest_over_time", {}).get("timeline_data", []):
                yield {
                    "keyword": keyword,
                    "date": item.get("date"),
                    "timestamp": item.get("timestamp"),
                    "values": item.get("values"),
                    "job_run_id": job_run_id
                }
    
    return trends