"""Media service for handling GCS frames and videos."""

import io
import os
import subprocess
import tempfile
from datetime import timedelta
from typing import List, Optional

from google.cloud import storage
from PIL import Image

from ..config import settings
from ..infrastructure import get_storage_client, get_ffmpeg_cmd, has_nvenc
from ..utils import temp_file_manager


class MediaService:
    """Service for downloading and processing media from GCS."""
    
    def __init__(self, client: storage.Client = None):
        """
        Initialize the media service.
        
        Args:
            client: Optional GCS client. If not provided, creates one.
        """
        self._client = client
    
    @property
    def client(self) -> storage.Client:
        """Lazy-load the GCS client."""
        if self._client is None:
            self._client = get_storage_client()
        return self._client
    
    def generate_signed_url(
        self, 
        gcs_uri: str, 
        expiration_seconds: int = None
    ) -> Optional[str]:
        """
        Generate a signed URL for a GCS object.
        
        Args:
            gcs_uri: GCS URI (gs://bucket/path/to/object)
            expiration_seconds: URL expiration time in seconds
            
        Returns:
            Signed URL string or None if error
        """
        if expiration_seconds is None:
            expiration_seconds = settings.signed_url_expiration
            
        if not gcs_uri or not gcs_uri.startswith("gs://"):
            return None
        
        try:
            # Parse GCS URI
            parts = gcs_uri.replace("gs://", "").split("/", 1)
            if len(parts) != 2:
                return None
            bucket_name, blob_name = parts
            
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Generate signed URL
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration_seconds),
                method="GET"
            )
            
            return url
            
        except Exception as e:
            print(f"Error generating signed URL for {gcs_uri}: {e}")
            return None
    
    def download_frame_as_image(self, gcs_uri: str) -> Optional[Image.Image]:
        """
        Download frame from GCS and return as PIL Image for Gradio display.
        
        Args:
            gcs_uri: GCS URI of the frame
            
        Returns:
            PIL Image or None if error
        """
        if not gcs_uri or not gcs_uri.startswith("gs://"):
            return None
        
        try:
            parts = gcs_uri.replace("gs://", "").split("/", 1)
            if len(parts) != 2:
                return None
            bucket_name, blob_name = parts
            
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            image_bytes = blob.download_as_bytes()
            return Image.open(io.BytesIO(image_bytes))
            
        except Exception as e:
            print(f"Error downloading frame from {gcs_uri}: {e}")
            return None
    
    def create_animated_gif_from_frames(
        self, 
        frame_uris: List[str], 
        fps: int = 3
    ) -> Optional[str]:
        """
        Download all frames from GCS and create an animated GIF.
        
        Args:
            frame_uris: List of GCS URIs for frames
            fps: Frames per second for the GIF (default 3)
            
        Returns:
            Path to temporary GIF file or None if error
        """
        if not frame_uris:
            return None
        
        try:
            frames: List[Image.Image] = []
            
            print(f"Downloading {len(frame_uris)} frames for GIF...")
            
            for uri in frame_uris:
                if not uri or not uri.startswith("gs://"):
                    continue
                
                try:
                    parts = uri.replace("gs://", "").split("/", 1)
                    if len(parts) != 2:
                        continue
                    bucket_name, blob_name = parts
                    
                    bucket = self.client.bucket(bucket_name)
                    blob = bucket.blob(blob_name)
                    
                    image_bytes = blob.download_as_bytes()
                    img = Image.open(io.BytesIO(image_bytes))
                    
                    # Convert to RGB if necessary (GIF doesn't support RGBA well)
                    if img.mode in ('RGBA', 'P'):
                        img = img.convert('RGB')
                    
                    frames.append(img)
                except Exception as e:
                    print(f"Warning: Failed to download frame {uri}: {e}")
                    continue
            
            if not frames:
                print("No frames downloaded successfully")
                return None
            
            # Create temporary GIF file
            temp_gif = tempfile.NamedTemporaryFile(suffix=".gif", delete=False)
            
            # Calculate duration per frame in milliseconds
            duration_ms = int(1000 / fps)
            
            # Save as animated GIF
            frames[0].save(
                temp_gif.name,
                save_all=True,
                append_images=frames[1:] if len(frames) > 1 else [],
                duration=duration_ms,
                loop=0  # Infinite loop
            )
            
            # Track for cleanup
            temp_file_manager.track_gif(temp_gif.name)
            
            print(f"✓ Created GIF with {len(frames)} frames at {fps} fps")
            return temp_gif.name
            
        except Exception as e:
            print(f"Error creating GIF: {e}")
            return None
    
    def download_video_to_temp(self, gcs_uri: str) -> Optional[str]:
        """
        Download video from GCS to a temporary file for Gradio display.
        
        Args:
            gcs_uri: GCS URI of the video
            
        Returns:
            Path to temporary video file or None if error
        """
        if not gcs_uri or not gcs_uri.startswith("gs://"):
            return None
        
        try:
            parts = gcs_uri.replace("gs://", "").split("/", 1)
            if len(parts) != 2:
                return None
            bucket_name, blob_name = parts
            
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Check if blob exists
            if not blob.exists():
                print(f"Video not found: {gcs_uri}")
                return None
            
            # Download video
            temp_download = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
            print(f"Downloading video...")
            blob.download_to_filename(temp_download.name)
            
            # Convert HEVC to H.264 for browser compatibility
            temp_output = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
            ffmpeg = get_ffmpeg_cmd()
            
            result = None
            
            if has_nvenc():
                # Try GPU encoding first (much faster)
                print(f"Converting HEVC to H.264 (GPU)...")
                gpu_cmd = [
                    ffmpeg, "-y", "-hwaccel", "cuda", "-i", temp_download.name,
                    "-c:v", "h264_nvenc", "-preset", "p1", "-b:v", "5M",
                    "-c:a", "copy",
                    "-movflags", "+faststart",
                    "-f", "mp4", "-loglevel", "error",
                    temp_output.name
                ]
                result = subprocess.run(gpu_cmd, capture_output=True, text=True)
                
                if result.returncode != 0:
                    print(f"GPU encoding failed, falling back to CPU...")
            
            if result is None or result.returncode != 0:
                # Fall back to CPU encoding
                print(f"Converting HEVC to H.264 (CPU)...")
                cpu_cmd = [
                    ffmpeg, "-y", "-i", temp_download.name,
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
                    "-c:a", "copy",
                    "-movflags", "+faststart",
                    "-f", "mp4", "-loglevel", "warning",
                    temp_output.name
                ]
                result = subprocess.run(cpu_cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"FFmpeg error: {result.stderr}")
                temp_file_manager.track_video(temp_download.name)
                return temp_download.name
            
            os.unlink(temp_download.name)
            print(f"Video ready: {temp_output.name} ({os.path.getsize(temp_output.name)} bytes)")
            temp_file_manager.track_video(temp_output.name)
            return temp_output.name
            
        except Exception as e:
            print(f"Error downloading video from {gcs_uri}: {e}")
            return None


# Global instance
media_service = MediaService()
