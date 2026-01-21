"""Allow running the package as a module: python -m bigquery_queries.anomaly_tracer"""

import sys
from pathlib import Path

# Ensure current directory is in path
_current_dir = Path(__file__).resolve().parent
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

from app import main

if __name__ == "__main__":
    main()
