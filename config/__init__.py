"""Configuration module for the CV Traceability Dashboard."""

import sys
from pathlib import Path

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import Settings, settings

__all__ = ["Settings", "settings"]
