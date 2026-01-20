import dlt
from serpapi import GoogleSearch

@dlt.source
def google_trends_source(api_key: str, keywords: list[str]):
    
    @dlt.resource(write_disposition="merge", primary_key=["keyword", "date"])
    def trends():
        for keyword in keywords:
            params = {
                "engine": "google_trends",
                "q": keyword,
                "api_key": api_key
            }
            search = GoogleSearch(params)
            results = search.get_dict()
            
            for item in results.get("interest_over_time", {}).get("timeline_data", []):
                yield {
                    "keyword": keyword,
                    "date": item.get("date"),
                    "timestamp": item.get("timestamp"),
                    "values": item.get("values")
                }
    
    return trends


# Run pipeline
pipeline = dlt.pipeline(
    pipeline_name="google_trends",
    destination="postgres",
    dataset_name="raw"
)

load_info = pipeline.run(
    google_trends_source(
        api_key="your_serpapi_key",
        keywords=["vpn", "antivirus", "ad blocker"]
    )
)

print(load_info)