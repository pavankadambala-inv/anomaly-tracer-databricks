"""
CV Inference Traceability Dashboard Package.

A Gradio web application to query and visualize Stage 1 and Stage 2 
inference results from BigQuery, with frame and video display.
"""

import sys
from pathlib import Path

# Ensure current directory is in path
_current_dir = Path(__file__).resolve().parent
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

# If being run directly (e.g., by Databricks Apps), launch the app
if __name__ == "__main__":
    from databricks_app import main
    main()
else:
    # Normal package import
    from config.settings import settings
    from ui.components import create_app
    
    __all__ = ["settings", "create_app"]
    __version__ = "1.0.0"
