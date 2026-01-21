"""Application settings and configuration constants."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class Settings:
    """Application configuration settings."""
    
    # Platform selection: "bigquery" or "databricks"
    platform: str = "databricks"
    
    # BigQuery settings (legacy - for reference)
    project_id: str = "invisible-animal-welfare"
    dataset_id: str = "cv_logs"
    
    # Databricks settings
    databricks_server_hostname: Optional[str] = None
    # Default HTTP path to your SQL Warehouse
    databricks_http_path: Optional[str] = "/sql/1.0/warehouses/1066550024e48b7a"
    databricks_access_token: Optional[str] = None
    
    # Catalog and schema for Unity Catalog
    catalog_name: str = "stage_cv_catalog"
    schema_name: str = "bronze"
    
    # Table names (without catalog.schema prefix)
    # UPDATE THESE to match your actual table names!
    stage1_table: str = "gemini_stage1_detections"
    stage2_table: str = "stage2_vlm_inferences"
    
    # Unity Catalog table paths (GCS paths for direct access)
    # These are the physical locations of Unity Catalog tables
    stage1_table_path: str = "gs://stg-cv/__unitystorage/catalogs/6e7f553f-6deb-4fde-a5b7-79043ade6dc8/tables/812795be-53d6-4a99-b066-f70cad607e9a"
    stage2_table_path: str = "gs://stg-cv/__unitystorage/catalogs/6e7f553f-6deb-4fde-a5b7-79043ade6dc8/tables/3ca3eb93-5e84-4ea0-a92e-a6b9984147b2"
    
    # GCS settings
    gcs_bucket: str = "animal-welfare-staging"
    signed_url_expiration: int = 3600  # seconds
    
    # Camera config directory (relative to this file's parent)
    camera_config_dir: Optional[Path] = None
    
    # FFmpeg settings
    ffmpeg_nvenc_path: Optional[Path] = None
    
    def __post_init__(self):
        """Set default paths after initialization."""
        if self.camera_config_dir is None:
            # Default to camera_config directory next to the package
            self.camera_config_dir = Path(__file__).parent.parent / "camera_config"
        
        if self.ffmpeg_nvenc_path is None:
            self.ffmpeg_nvenc_path = Path.home() / ".local" / "bin" / "ffmpeg-nvenc"
        
        # Load Databricks settings from environment if not set
        # Support both DATABRICKS_SERVER_HOSTNAME and DATABRICKS_HOST (Databricks Apps provides the latter)
        if self.databricks_server_hostname is None:
            self.databricks_server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST")
        if self.databricks_http_path is None:
            self.databricks_http_path = os.getenv("DATABRICKS_HTTP_PATH")
        if self.databricks_access_token is None:
            # Databricks Apps provides client credentials, not a token
            self.databricks_access_token = os.getenv("DATABRICKS_TOKEN")
    
    @property
    def full_stage1_table(self) -> str:
        """Get fully qualified table name for Stage 1."""
        if self.platform == "databricks":
            return f"{self.catalog_name}.{self.schema_name}.{self.stage1_table}"
        else:
            return f"{self.project_id}.{self.dataset_id}.{self.stage1_table}"
    
    @property
    def full_stage2_table(self) -> str:
        """Get fully qualified table name for Stage 2."""
        if self.platform == "databricks":
            return f"{self.catalog_name}.{self.schema_name}.{self.stage2_table}"
        else:
            return f"{self.project_id}.{self.dataset_id}.{self.stage2_table}"


# Global settings instance
settings = Settings()
