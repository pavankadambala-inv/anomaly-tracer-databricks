"""Infrastructure module for external service clients."""

import sys
from pathlib import Path

# Add parent to path for config import
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings
from .ffmpeg import setup_ffmpeg_nvenc, get_ffmpeg_cmd, has_nvenc

# Import appropriate clients based on platform
if settings.platform == "databricks":
    from .databricks_client import get_databricks_connection, get_databricks_config
    from .databricks_storage import get_storage_client
    
    __all__ = [
        "get_databricks_connection",
        "get_databricks_config",
        "get_storage_client",
        "setup_ffmpeg_nvenc",
        "get_ffmpeg_cmd",
        "has_nvenc",
    ]
else:
    from .bigquery_client import get_bigquery_client
    from .gcs_client import get_storage_client
    
    __all__ = [
        "get_bigquery_client",
        "get_storage_client", 
        "setup_ffmpeg_nvenc",
        "get_ffmpeg_cmd",
        "has_nvenc",
    ]
