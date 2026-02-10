#!/usr/bin/env python3
"""
CV Inference Traceability Dashboard for Databricks

A Gradio web application to query and visualize Stage 1 and Stage 2 
inference results from Databricks Unity Catalog tables.

This is the Databricks-specific entry point that can be run as a Databricks App.

Usage in Databricks:
    1. Upload this directory to Databricks Workspace or Repos
    2. Create a Databricks App pointing to this file
    3. Set environment variables:
       - DATABRICKS_SERVER_HOSTNAME
       - DATABRICKS_HTTP_PATH
       - DATABRICKS_TOKEN (or use Databricks authentication)
    4. The app will launch on the configured port
"""

import os
import sys
from pathlib import Path

# Ensure the package is in the path
_current_dir = Path(__file__).resolve().parent
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

# WORKAROUND: Patch gradio_client to fix API schema bug
# This prevents APIInfoParseError: Cannot parse schema True/False
try:
    import gradio_client.utils as gradio_utils
    
    # Patch get_type to handle non-dict schemas
    _original_get_type = gradio_utils.get_type
    def _patched_get_type(schema):
        if not isinstance(schema, dict):
            return "Any"
        return _original_get_type(schema)
    gradio_utils.get_type = _patched_get_type
    
    # Patch _json_schema_to_python_type to handle boolean schemas
    _original_json_schema_to_python_type = gradio_utils._json_schema_to_python_type
    def _patched_json_schema_to_python_type(schema, defs=None):
        if schema is True:
            return "Any"
        if schema is False:
            return "None"
        if not isinstance(schema, dict):
            return "Any"
        try:
            return _original_json_schema_to_python_type(schema, defs)
        except Exception:
            return "Any"
    gradio_utils._json_schema_to_python_type = _patched_json_schema_to_python_type
    
    # Also patch the main function
    def _patched_json_schema_to_python_type_main(schema):
        if schema is True:
            return "Any"
        if schema is False:
            return "None"
        if not isinstance(schema, dict):
            return "Any"
        try:
            return _patched_json_schema_to_python_type(schema, schema.get("$defs"))
        except Exception:
            return "Any"
    gradio_utils.json_schema_to_python_type = _patched_json_schema_to_python_type_main
    
    print("✓ Applied Gradio API schema patch")
except Exception as e:
    print(f"Note: Could not patch gradio_client: {e}")

from config import settings
from infrastructure import setup_ffmpeg_nvenc
from services import camera_config_service
from ui import create_app


def configure_gcp_credentials():
    """Configure GCP credentials from environment or Databricks Secrets."""
    import tempfile
    
    # Debug: Show all GCP-related env vars
    print(f"  DEBUG: GCP_SERVICE_ACCOUNT_JSON set: {bool(os.getenv('GCP_SERVICE_ACCOUNT_JSON'))}")
    if os.getenv('GCP_SERVICE_ACCOUNT_JSON'):
        print(f"  DEBUG: GCP_SERVICE_ACCOUNT_JSON length: {len(os.getenv('GCP_SERVICE_ACCOUNT_JSON'))}")
    
    # Option 1: GCP_SERVICE_ACCOUNT_JSON environment variable (from app.yaml)
    gcp_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if gcp_json:
        try:
            # Write JSON to temp file
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            temp_file.write(gcp_json)
            temp_file.close()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
            print(f"✓ GCP credentials loaded from GCP_SERVICE_ACCOUNT_JSON env var")
            return True
        except Exception as e:
            print(f"Warning: Could not parse GCP_SERVICE_ACCOUNT_JSON: {e}")
    
    # Option 2: Try to get from Databricks Secrets
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        try:
            secret_value = w.secrets.get_secret(scope="gcp-credentials", key="service-account-key")
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            temp_file.write(secret_value.value)
            temp_file.close()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
            print(f"✓ GCP credentials loaded from Databricks Secrets")
            return True
        except Exception as e:
            print(f"Note: Could not load from Databricks Secrets: {e}")
    except ImportError:
        print("Note: Databricks SDK not available for secrets")
    
    # Option 3: Use GOOGLE_APPLICATION_CREDENTIALS env var (file path)
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if os.path.exists(cred_path):
            print(f"✓ GCP credentials found at: {cred_path}")
            return True
        else:
            print(f"Warning: GOOGLE_APPLICATION_CREDENTIALS points to non-existent file: {cred_path}")
    
    # Option 4: Check for service account JSON in standard locations
    possible_paths = [
        "/dbfs/secrets/gcp-key.json",
        "/dbfs/FileStore/secrets/gcp-key.json",
        str(Path.home() / ".config/gcloud/application_default_credentials.json"),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
            print(f"✓ GCP credentials found at: {path}")
            return True
    
    print("Warning: No GCP credentials found")
    print("  Frames and videos may not be accessible")
    print("  Set GCP_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS")
    return False


def main():
    """Main entry point for the Databricks application."""
    print("=" * 60)
    print("CV Inference Traceability Dashboard (Databricks)")
    print("=" * 60)
    print(f"Platform: {settings.platform}")
    print(f"Catalog: {settings.catalog_name}")
    print(f"Schema: {settings.schema_name}")
    print(f"Stage 1 Table: {settings.full_stage1_table}")
    print(f"Stage 2 Table: {settings.full_stage2_table}")
    print()
    
    # Verify Databricks connection settings
    if not settings.databricks_server_hostname:
        print("ERROR: DATABRICKS_SERVER_HOSTNAME not set!")
        print("Please set environment variable DATABRICKS_SERVER_HOSTNAME")
        sys.exit(1)
    
    if not settings.databricks_http_path:
        print("ERROR: DATABRICKS_HTTP_PATH not set!")
        print("Please set environment variable DATABRICKS_HTTP_PATH")
        sys.exit(1)
    
    print(f"Databricks Server: {settings.databricks_server_hostname}")
    print(f"HTTP Path: {settings.databricks_http_path}")
    print()
    
    # Configure GCP credentials for GCS access
    print("Checking GCP credentials for GCS access...")
    configure_gcp_credentials()
    print()
    
    # Load camera configuration
    print("Loading camera configuration...")
    try:
        camera_config_service.load()
        print(f"✓ Loaded camera configuration")
    except Exception as e:
        print(f"Warning: Could not load camera config: {e}")
        print("  (Camera names will show as IDs)")
    print()
    
    # Setup ffmpeg with NVENC for fast video conversion
    print("Checking FFmpeg setup...")
    try:
        setup_ffmpeg_nvenc()
    except Exception as e:
        print(f"Warning: FFmpeg setup issue: {e}")
        print("  (Video conversion may be slower)")
    print()
    
    # Create and launch the app
    print("Creating Gradio app...")
    app = create_app()
    
    # Get port from environment (Databricks Apps use specific ports)
    port = int(os.getenv("GRADIO_SERVER_PORT", "7860"))
    
    print(f"Launching app on port {port}...")
    print("=" * 60)
    
    # For Databricks Apps behind reverse proxy
    # Queue is needed for proper request handling
    app.queue()
    
    # WORKAROUND: Prevent Gradio from checking localhost accessibility
    # by monkey-patching the networking check
    try:
        import gradio.networking as networking
        networking.url_ok = lambda url: True
        print("✓ Applied Gradio networking patch")
    except Exception as e:
        print(f"Note: Could not patch networking: {e}")
    
    app.launch(
        server_name="0.0.0.0",
        server_port=port,
        share=False,
        show_error=True,
        show_api=False,  # Disable API docs to avoid schema serialization issues
        # For Databricks Apps behind reverse proxy
        root_path=os.getenv("GRADIO_ROOT_PATH", ""),
        # Additional settings for Databricks
        favicon_path=None,
        ssl_verify=False,
        quiet=False,
    )


if __name__ == "__main__":
    main()
