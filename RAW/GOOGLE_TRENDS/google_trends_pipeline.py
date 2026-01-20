import dlt
import serpapi
import sys 
import os 
import logging

logger = logging.getLogger(__name__)
base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '../../'
    )
)
sys.path.append(base_path)
from project_files import functions
schema_contract ={
        "tables": "freeze"
        , "columns": "evolve"
        , "data_type": "freeze"
    }

config_dictionary = functions.read_config_segment(segment='GOOGLE_TRENDS')
conn_info = functions.read_config_segment(segment='POSTGRESQL_CONN')  # Add your connection section name

def construct_date_range(): 
    current_date = functions.get_current_date()
    days_overlap = -int(config_dictionary['DAYS_OVERLAP'])
    current_date_minus_overlap = functions.date_add(current_date, days=days_overlap)
    date_range = current_date_minus_overlap + ' ' + current_date
    return date_range


@dlt.source(
    schema_contract=schema_contract
    )
def google_trends_source(keywords: list[str]):
    
    @dlt.resource(write_disposition="merge", primary_key=["keyword", "date"])
    def trends():
        for keyword in keywords:
            params = {
                "engine": "google_trends",
                "q": keyword,
                "date": construct_date_range(),
                "geo": config_dictionary['COUNTRY'],
                "api_key": config_dictionary['API_KEY'],
                "tz": int(config_dictionary['TIMEZONE'])
            }
            
            search = serpapi.search(params)
            results = search.as_dict()
            
            for item in results.get("interest_over_time", {}).get("timeline_data", []):
                yield {
                    "keyword": keyword,
                    "date": item.get("date"),
                    "timestamp": item.get("timestamp"),
                    "values": item.get("values")
                }
    
    return trends


# Build connection string from existing config
pg_credentials = f"postgresql://{conn_info['USERNAME']}:{conn_info['PASSWORD']}@{conn_info['HOST_NAME']}:{conn_info['PORT_NUMBER']}/{conn_info['DATABASE']}"

# Run pipeline
pipeline = dlt.pipeline(
    pipeline_name="google_trends",
    destination=dlt.destinations.postgres(credentials=pg_credentials),
    dataset_name="raw"
)

load_info = pipeline.run(
    google_trends_source(keywords=["vpn", "antivirus", "ad blocker"])
)

print(load_info)