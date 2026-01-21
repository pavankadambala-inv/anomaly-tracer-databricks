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
    
    # Get OAuth credentials from environment (Databricks Apps provides these)
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
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
    
    print(f"Connecting to Databricks SQL...")
    print(f"  Server: {server_hostname}")
    print(f"  HTTP Path: {http_path}")
    print(f"  Has Token: {bool(access_token)}")
    print(f"  Has OAuth: {bool(client_id and client_secret)}")
    
    # Try different authentication methods
    try:
        if access_token:
            # Method 1: Use PAT token authentication
            print(f"  Using: Token authentication")
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token,
            )
        elif client_id and client_secret:
            # Method 2: Use OAuth M2M (machine-to-machine) authentication
            print(f"  Using: OAuth M2M authentication")
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                client_id=client_id,
                client_secret=client_secret,
            )
        else:
            # Method 3: Try default SDK authentication
            print(f"  Using: Default SDK authentication")
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
            )
        
        print(f"  ✓ Connected successfully!")
        return connection
    except Exception as e:
        print(f"  ✗ Connection error: {e}")
        raise


def get_workspace_client() -> WorkspaceClient:
    """
    Get Databricks Workspace client for API operations.
    
    Returns:
        Databricks WorkspaceClient
    """
    return WorkspaceClient()
