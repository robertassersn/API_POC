"""Google Trends dlt source."""
import dlt
import serpapi
import time
import logging
from typing import Iterator

from project_files import functions
from ingestion.config.google_trends_config import construct_date_range, SCHEMA_CONTRACT

logger = logging.getLogger(__name__)


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
def google_trends_source(config: dict, keywords: list[str]):
    """Google Trends data source."""
    
    @dlt.resource(write_disposition="merge", primary_key=["keyword", "date"])
    def trends() -> Iterator[dict]:
        for keyword in keywords:
            params = {
                "engine": "google_trends",
                "q": keyword,
                "date": construct_date_range(),
                "geo": config['COUNTRY'],
                "api_key": config['API_KEY'],
                "tz": int(config['TIMEZONE'])
            }
            
            results = fetch_with_retry(params)
                        
            for item in results.get("interest_over_time", {}).get("timeline_data", []):
                yield {
                    "keyword": keyword,
                    "date": item.get("date"),
                    "timestamp": item.get("timestamp"),
                    "values": item.get("values")
                }
    
    return trends