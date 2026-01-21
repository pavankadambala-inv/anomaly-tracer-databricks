"""BigQuery client factory and connection management."""

import sys
from pathlib import Path

from google.cloud import bigquery

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings


def get_bigquery_client() -> bigquery.Client:
    """Get authenticated BigQuery client."""
    return bigquery.Client(project=settings.project_id)
