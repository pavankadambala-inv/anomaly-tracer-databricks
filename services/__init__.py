"""Services module for business logic."""

from ..config import settings
from .camera_config import CameraConfigService, camera_config_service
from .media_service import MediaService, media_service

# Import the appropriate query service based on platform
if settings.platform == "databricks":
    from .databricks_query_service import DatabricksQueryService as QueryService
    from .databricks_query_service import databricks_query_service as query_service
else:
    from .query_service import QueryService, query_service

__all__ = [
    "CameraConfigService",
    "camera_config_service",
    "QueryService", 
    "query_service",
    "MediaService",
    "media_service",
]
