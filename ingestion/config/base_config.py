"""Global configuration helpers."""
import os
import sys

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_path)

from project_files import functions

_conn_cache = {}


def get_conn_info(segment: str = 'POSTGRESQL_CONN') -> dict:
    """Get connection info (cached)."""
    if segment not in _conn_cache:
        _conn_cache[segment] = functions.read_config_segment(segment=segment)
    return _conn_cache[segment]


def get_pg_credentials(segment: str = 'POSTGRESQL_CONN') -> str:
    """Build PostgreSQL connection string."""
    conn = get_conn_info(segment)
    return f"postgresql://{conn['USERNAME']}:{conn['PASSWORD']}@{conn['HOST_NAME']}:{conn['PORT_NUMBER']}/{conn['DATABASE']}"