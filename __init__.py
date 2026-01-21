"""
CV Inference Traceability Dashboard Package.

A Gradio web application to query and visualize Stage 1 and Stage 2 
inference results from BigQuery, with frame and video display.
"""

import sys
from pathlib import Path

# If running as standalone, add to path and use absolute imports
if __name__ == "__main__":
    _current_dir = Path(__file__).resolve().parent
    if str(_current_dir) not in sys.path:
        sys.path.insert(0, str(_current_dir))
    
    # Run the databricks app
    import databricks_app
    databricks_app.main()
else:
    # Normal package imports
    try:
        from .config import settings
        from .ui import create_app
    except ImportError:
        from config import settings
        from ui import create_app

    __all__ = ["settings", "create_app"]
    __version__ = "1.0.0"
