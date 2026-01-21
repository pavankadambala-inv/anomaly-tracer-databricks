"""BigQuery client factory and connection management."""

from google.cloud import bigquery

from ..config import settings


def get_bigquery_client() -> bigquery.Client:
    """Get authenticated BigQuery client."""
    return bigquery.Client(project=settings.project_id)
