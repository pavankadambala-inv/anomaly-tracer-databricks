"""Services module for business logic."""

import sys
from pathlib import Path

# Add parent to path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

# Use absolute imports
from config.settings import settings
from services.camera_config import CameraConfigService, camera_config_service
from services.media_service import MediaService, media_service

# Import the appropriate query service based on platform
if settings.platform == "databricks":
    from services.databricks_query_service import DatabricksQueryService as QueryService
    from services.databricks_query_service import databricks_query_service as query_service
else:
    from services.query_service import QueryService, query_service

__all__ = [
    "CameraConfigService",
    "camera_config_service",
    "QueryService", 
    "query_service",
    "MediaService",
    "media_service",
]
