"""Google Cloud Storage client factory."""

from google.cloud import storage

from ..config import settings


def get_storage_client() -> storage.Client:
    """Get authenticated GCS client."""
    return storage.Client(project=settings.project_id)
