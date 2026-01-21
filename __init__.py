"""
CV Inference Traceability Dashboard Package.

A Gradio web application to query and visualize Stage 1 and Stage 2 
inference results from BigQuery, with frame and video display.
"""

from .config import settings
from .ui import create_app

__all__ = ["settings", "create_app"]
__version__ = "1.0.0"
