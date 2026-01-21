"""Application state management for the UI."""

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class AppState:
    """
    Container for application state.
    
    This replaces global variables with a structured state object.
    """
    query_results: pd.DataFrame = field(default_factory=pd.DataFrame)


# Global state instance
app_state = AppState()
