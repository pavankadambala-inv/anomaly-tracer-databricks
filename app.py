#!/usr/bin/env python3
"""
CV Inference Traceability Dashboard

A Gradio web application to query and visualize Stage 1 and Stage 2 
inference results from BigQuery or Databricks, with frame and video display.

Usage:
    # From cv-detection-logic directory:
    uv run python -m bigquery_queries.anomaly_tracer
    
    # Or directly from this directory:
    uv run python app.py
    
    # For Databricks deployment:
    python databricks_app.py
    
Then open http://localhost:7860 in your browser.
"""

import sys
from pathlib import Path

# Add current directory to path
_current_dir = Path(__file__).resolve().parent
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

# Use absolute imports
from config.settings import settings
from infrastructure import setup_ffmpeg_nvenc
from services import camera_config_service
from ui import create_app


def main():
    """Main entry point for the application."""
    print("=" * 60)
    print("CV Inference Traceability Dashboard")
    print("=" * 60)
    print(f"Platform: {settings.platform}")
    
    if settings.platform == "databricks":
        print(f"Catalog: {settings.catalog_name}")
        print(f"Schema: {settings.schema_name}")
        print(f"Stage 1 Table: {settings.full_stage1_table}")
        print(f"Stage 2 Table: {settings.full_stage2_table}")
    else:
        print(f"Project: {settings.project_id}")
        print(f"Dataset: {settings.dataset_id}")
    print()
    
    # Verify connection settings for Databricks
    if settings.platform == "databricks":
        if not settings.databricks_server_hostname:
            print("WARNING: DATABRICKS_SERVER_HOSTNAME not set!")
            print("Please set environment variable DATABRICKS_SERVER_HOSTNAME")
        if not settings.databricks_http_path:
            print("WARNING: DATABRICKS_HTTP_PATH not set!")
            print("Please set environment variable DATABRICKS_HTTP_PATH")
        print()
    
    # Load camera configuration
    print("Loading camera configuration...")
    try:
        camera_config_service.load()
        print("✓ Camera configuration loaded")
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
    
    print("Launching app on port 7860...")
    print("=" * 60)
    
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        show_error=True
    )


if __name__ == "__main__":
    main()
