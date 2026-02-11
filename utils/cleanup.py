"""Temporary file management and cleanup utilities."""

import atexit
import os
from typing import List


class TempFileManager:
    """
    Manages temporary files with automatic cleanup.
    Uses LRU policy - when max files reached, oldest files are deleted.
    """
    
    def __init__(self, max_videos: int = 10, max_gifs: int = 20):
        """
        Initialize temp file manager.
        
        Args:
            max_videos: Maximum videos to cache (default 10)
            max_gifs: Maximum GIFs to cache (default 20)
        """
        self._temp_video_files: List[str] = []
        self._temp_gif_files: List[str] = []
        self._max_videos = max_videos
        self._max_gifs = max_gifs
        # Register cleanup on exit
        atexit.register(self.cleanup)
    
    def track_video(self, filepath: str) -> None:
        """
        Track a temporary video file for cleanup.
        Automatically removes oldest videos when limit is reached.
        """
        self._temp_video_files.append(filepath)
        
        # Auto-cleanup when limit reached
        if len(self._temp_video_files) > self._max_videos:
            files_to_remove = self._temp_video_files[:-self._max_videos]
            print(f"🧹 Cleaning up {len(files_to_remove)} old videos (keeping last {self._max_videos})...")
            for old_file in files_to_remove:
                try:
                    if os.path.exists(old_file):
                        os.unlink(old_file)
                except Exception:
                    pass
            self._temp_video_files = self._temp_video_files[-self._max_videos:]
    
    def track_gif(self, filepath: str) -> None:
        """
        Track a temporary GIF file for cleanup.
        Automatically removes oldest GIFs when limit is reached.
        """
        self._temp_gif_files.append(filepath)
        
        # Auto-cleanup when limit reached
        if len(self._temp_gif_files) > self._max_gifs:
            files_to_remove = self._temp_gif_files[:-self._max_gifs]
            print(f"🧹 Cleaning up {len(files_to_remove)} old GIFs (keeping last {self._max_gifs})...")
            for old_file in files_to_remove:
                try:
                    if os.path.exists(old_file):
                        os.unlink(old_file)
                except Exception:
                    pass
            self._temp_gif_files = self._temp_gif_files[-self._max_gifs:]
    
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
