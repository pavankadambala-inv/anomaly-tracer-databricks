"""Infrastructure module for external service clients."""

import sys
from pathlib import Path

# Ensure parent is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

# Import settings to check platform
from config.settings import settings

# Import appropriate clients based on platform
if settings.platform == "databricks":
    from infrastructure.databricks_client import get_databricks_connection, get_workspace_client
    from infrastructure.databricks_storage import get_storage_client
    
    __all__ = [
        "get_databricks_connection",
        "get_workspace_client",
        "get_storage_client",
    ]
else:
    from infrastructure.bigquery_client import get_bigquery_client
    from infrastructure.gcs_client import get_storage_client
    
    __all__ = [
        "get_bigquery_client",
        "get_storage_client",
    ]
