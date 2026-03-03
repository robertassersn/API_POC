"""Google Trends specific configuration."""
import os
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)

from project_files import functions

_config = None

# SCHEMA_CONTRACT = {
#     "tables": "freeze",
#     "columns": "evolve",
#     "data_type": "freeze"
# }


def get_config() -> dict:
    global _config
    if _config is None:
        _config = functions.read_config_segment(segment='GOOGLE_TRENDS')
        os.makedirs(_config['DIR_DOWNLOADED_FILES'], exist_ok=True)
    return _config


def construct_date_range(DAYS_OVERLAP) -> str:
    """Build date range string for API."""
    config = get_config()
    current_date = functions.get_current_date()
    days_overlap = -int(DAYS_OVERLAP)
    current_date_minus_overlap = functions.date_add(current_date, days=days_overlap)
    return f"{current_date_minus_overlap} {current_date}"