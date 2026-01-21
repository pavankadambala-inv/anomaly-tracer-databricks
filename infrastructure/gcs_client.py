"""Google Cloud Storage client factory."""

import sys
from pathlib import Path

from google.cloud import storage

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings


def get_storage_client() -> storage.Client:
    """Get authenticated GCS client."""
    return storage.Client(project=settings.project_id)
