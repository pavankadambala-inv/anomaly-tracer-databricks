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

from config import settings
from infrastructure import setup_ffmpeg_nvenc
from services import camera_config_service
from ui import create_app


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
    
    app.launch(
        server_name="0.0.0.0",
        server_port=port,
        share=False,
        show_error=True,
        # For Databricks Apps, we might need to set root_path
        root_path=os.getenv("GRADIO_ROOT_PATH", None)
    )


if __name__ == "__main__":
    main()
