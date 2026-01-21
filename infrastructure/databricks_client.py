"""Databricks SQL client factory and connection management."""

import os
import sys
from pathlib import Path
from typing import Optional

from databricks import sql
from databricks.sdk import WorkspaceClient

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings


def get_databricks_connection():
    """
    Get authenticated Databricks SQL connection.
    
    Uses Databricks authentication from environment.
    In Databricks Apps, uses DATABRICKS_HOST and OAuth client credentials automatically.
    
    Returns:
        Databricks SQL connection
    """
    # Get connection parameters from settings or environment
    # DATABRICKS_HOST is provided by Databricks Apps
    server_hostname = (
        settings.databricks_server_hostname 
        or os.getenv("DATABRICKS_SERVER_HOSTNAME") 
        or os.getenv("DATABRICKS_HOST")
    )
    http_path = settings.databricks_http_path or os.getenv("DATABRICKS_HTTP_PATH")
    access_token = settings.databricks_access_token or os.getenv("DATABRICKS_TOKEN")
    
    if not server_hostname:
        raise ValueError(
            "Databricks server hostname not configured. "
            "Please set DATABRICKS_SERVER_HOSTNAME or DATABRICKS_HOST environment variable."
        )
    
    if not http_path:
        raise ValueError(
            "Databricks HTTP path not configured. "
            "Please set DATABRICKS_HTTP_PATH environment variable. "
            "Example: /sql/1.0/warehouses/your-warehouse-id"
        )
    
    # Try different authentication methods
    try:
        if access_token:
            # Use token authentication
            print(f"Connecting with token authentication...")
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token,
            )
        else:
            # Use OAuth (client credentials) - Databricks Apps provides these automatically
            # DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are set by Databricks Apps
            print(f"Connecting with OAuth (Databricks SDK)...")
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                # Let databricks-sql-connector use SDK authentication
                credentials_provider=_get_credentials_provider(),
            )
        return connection
    except Exception as e:
        print(f"Connection error: {e}")
        raise


def _get_credentials_provider():
    """Get credentials provider using Databricks SDK."""
    from databricks.sdk.core import Config, oauth_service_principal
    
    config = Config()
    
    # Use OAuth service principal if client credentials are available
    if config.client_id and config.client_secret:
        return oauth_service_principal(config)
    
    # Fall back to default credentials
    return None


def get_workspace_client() -> WorkspaceClient:
    """
    Get Databricks Workspace client for API operations.
    
    Returns:
        Databricks WorkspaceClient
    """
    return WorkspaceClient()
