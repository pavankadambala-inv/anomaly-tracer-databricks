"""UI module for Gradio components and handlers."""

import sys
from pathlib import Path

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from ui.components import create_app
from ui.handlers import run_query, get_row_details, load_filters, update_cameras_on_farm_change
from ui.formatters import format_results_for_display

__all__ = [
    "create_app",
    "run_query",
    "get_row_details",
    "load_filters",
    "update_cameras_on_farm_change",
    "format_results_for_display",
]
