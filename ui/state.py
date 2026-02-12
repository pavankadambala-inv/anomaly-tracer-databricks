"""Application state management for the UI."""

from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional

import pandas as pd


@dataclass
class AppState:
    """
    Container for application state.
    
    This replaces global variables with a structured state object.
    """
    query_results: pd.DataFrame = field(default_factory=pd.DataFrame)
    # Cache for row details to prevent redundant downloads
    # Key: row_index, Value: (gif_path, video_path, details_text)
    row_cache: Dict[int, Tuple[Optional[str], Optional[str], str]] = field(default_factory=dict)
    last_selected_row: Optional[int] = None


# Global state instance
app_state = AppState()
