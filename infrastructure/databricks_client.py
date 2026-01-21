"""Databricks SQL client factory and connection management."""

import os
import sys
from pathlib import Path
from typing import Optional

from databricks import sql
from databricks.sdk.core import Config

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings


def get_databricks_connection():
    """
    Get authenticated Databricks SQL connection.
    
    Uses Databricks authentication from environment or ~/.databrickscfg
    
    Returns:
        Databricks SQL connection
    """
    # Get connection parameters from settings or environment
    server_hostname = settings.databricks_server_hostname or os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = settings.databricks_http_path or os.getenv("DATABRICKS_HTTP_PATH")
    access_token = settings.databricks_access_token or os.getenv("DATABRICKS_TOKEN")
    
    if not server_hostname or not http_path:
        raise ValueError(
            "Databricks connection parameters not configured. "
            "Please set databricks_server_hostname and databricks_http_path in settings "
            "or set DATABRICKS_SERVER_HOSTNAME and DATABRICKS_HTTP_PATH environment variables."
        )
    
    # Create connection
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        # Alternatively, use credentials=lambda: access_token if access_token else None
    )
    
    return connection


def get_databricks_config() -> Config:
    """
    Get Databricks SDK config for file operations.
    
    Returns:
        Databricks SDK Config object
    """
    return Config()
