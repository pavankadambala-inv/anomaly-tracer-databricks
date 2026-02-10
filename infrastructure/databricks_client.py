"""Databricks SQL client factory and connection management."""

import os
import sys
from pathlib import Path
from typing import Optional

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings


def get_databricks_connection():
    """
    Get authenticated Databricks SQL connection.
    
    Uses Databricks SDK's unified authentication which automatically
    detects and uses the available credentials (OAuth, PAT, etc.)
    
    Returns:
        Databricks SQL connection
    """
    # Get HTTP path from settings or environment
    http_path = settings.databricks_http_path or os.getenv("DATABRICKS_HTTP_PATH")
    
    if not http_path:
        raise ValueError(
            "Databricks HTTP path not configured. "
            "Please set DATABRICKS_HTTP_PATH environment variable. "
            "Example: /sql/1.0/warehouses/your-warehouse-id"
        )
    
    print(f"Connecting to Databricks SQL...")
    print(f"  HTTP Path: {http_path}")
    
    # Show which credentials are being used
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "NOT SET")
    print(f"  Client ID from env: {client_id[:12]}..." if len(client_id) > 12 else f"  Client ID: {client_id}")
    
    # Use Databricks SDK unified authentication
    # This automatically picks up credentials from environment:
    # - DATABRICKS_HOST
    # - DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (OAuth M2M)
    # - DATABRICKS_TOKEN (PAT)
    # - Or other configured auth methods
    
    try:
        # Use Databricks SDK to handle authentication
        from databricks.sdk import WorkspaceClient
        
        # Create SDK client (this handles OAuth M2M, PAT, etc. automatically)
        w = WorkspaceClient()
        cfg = w.config
        
        print(f"  Host: {cfg.host}")
        print(f"  Auth Type: {cfg.auth_type}")
        print(f"  SDK Client ID: {cfg.client_id[:12] if cfg.client_id else 'None'}...")
        
        # Get authentication token from SDK
        print(f"  Getting authentication token...")
        auth_headers = cfg.authenticate()
        
        if not auth_headers or not isinstance(auth_headers, dict):
            raise ValueError(f"cfg.authenticate() returned invalid data: {type(auth_headers)}")
        
        auth_header = auth_headers.get("Authorization")
        if not auth_header:
            raise ValueError(f"No Authorization header. Available headers: {list(auth_headers.keys())}")
        
        if not auth_header.startswith("Bearer "):
            raise ValueError(f"Authorization header doesn't start with 'Bearer ': {auth_header[:50]}")
        
        # Extract token (remove "Bearer " prefix)
        access_token = auth_header[7:]
        print(f"  ✓ Got access token (first 20 chars): {access_token[:20]}...")
        
        # Connect using the access token directly
        # Note: Using access_token instead of credentials_provider avoids 
        # compatibility issues with the databricks-sql-connector
        print(f"  Attempting connection...")
        connection = sql.connect(
            server_hostname=cfg.host.replace("https://", "").replace("http://", ""),
            http_path=http_path,
            access_token=access_token,
        )
        
        print(f"  ✓ Connection object created!")
        
        # Test the connection with a simple query
        print(f"  Testing connection with simple query...")
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                print(f"  ✓ Test query successful! Result: {result}")
        except Exception as test_error:
            print(f"  ✗ Test query failed: {test_error}")
            raise
        
        print(f"  ✓ Connection fully verified!")
        return connection
        
    except Exception as e:
        print(f"  ✗ Connection error: {e}")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error details: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def get_workspace_client() -> WorkspaceClient:
    """
    Get Databricks Workspace client for API operations.
    
    Returns:
        Databricks WorkspaceClient
    """
    return WorkspaceClient()
