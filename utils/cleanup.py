"""Temporary file management and cleanup utilities."""

import atexit
import os
from typing import List


class TempFileManager:
    """Manages temporary files and ensures cleanup on exit."""
    
    def __init__(self):
        self._temp_video_files: List[str] = []
        self._temp_gif_files: List[str] = []
        # Register cleanup on exit
        atexit.register(self.cleanup)
    
    def track_video(self, filepath: str) -> None:
        """Track a temporary video file for cleanup."""
        self._temp_video_files.append(filepath)
    
    def track_gif(self, filepath: str) -> None:
        """Track a temporary GIF file for cleanup."""
        self._temp_gif_files.append(filepath)
    
    def cleanup(self) -> None:
        """Clean up all temporary video and GIF files."""
        total = len(self._temp_video_files) + len(self._temp_gif_files)
        if total > 0:
            print(f"\n🧹 Cleaning up {total} temporary files...")
            for filepath in self._temp_video_files + self._temp_gif_files:
                try:
                    if os.path.exists(filepath):
                        os.unlink(filepath)
                except Exception:
                    pass
            self._temp_video_files.clear()
            self._temp_gif_files.clear()
            print("✓ Cleanup complete")


# Global instance
temp_file_manager = TempFileManager()
