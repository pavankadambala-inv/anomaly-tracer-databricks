"""Databricks file storage access for Unity Catalog tables and GCS."""

import io
from typing import Optional

from google.cloud import storage
from PIL import Image

from ..config import settings


def get_storage_client() -> storage.Client:
    """
    Get GCS client for accessing Unity Catalog table files.
    
    Unity Catalog tables in Databricks are stored in GCS with paths like:
    gs://stg-cv/__unitystorage/catalogs/{catalog_id}/tables/{table_id}
    
    Returns:
        Google Cloud Storage client
    """
    # For Databricks on GCP, we can use GCS client directly
    # Authentication should be configured via environment or service account
    return storage.Client()


def download_file_from_gcs(gcs_uri: str) -> Optional[bytes]:
    """
    Download a file from GCS.
    
    Args:
        gcs_uri: GCS URI (gs://bucket/path/to/file)
        
    Returns:
        File bytes or None if error
    """
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        return None
    
    try:
        parts = gcs_uri.replace("gs://", "").split("/", 1)
        if len(parts) != 2:
            return None
        bucket_name, blob_name = parts
        
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        return blob.download_as_bytes()
    except Exception as e:
        print(f"Error downloading file from {gcs_uri}: {e}")
        return None


def check_file_exists(gcs_uri: str) -> bool:
    """
    Check if a file exists in GCS.
    
    Args:
        gcs_uri: GCS URI (gs://bucket/path/to/file)
        
    Returns:
        True if file exists, False otherwise
    """
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        return False
    
    try:
        parts = gcs_uri.replace("gs://", "").split("/", 1)
        if len(parts) != 2:
            return False
        bucket_name, blob_name = parts
        
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        return blob.exists()
    except Exception as e:
        print(f"Error checking file existence {gcs_uri}: {e}")
        return False
