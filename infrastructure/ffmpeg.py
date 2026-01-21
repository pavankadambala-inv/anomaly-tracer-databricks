"""FFmpeg setup with NVENC (GPU acceleration) support."""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Ensure parent directory is in path for imports
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings

# Module-level state for NVENC availability (use a dict so imports get a reference)
_state = {"has_nvenc": False}


def has_nvenc() -> bool:
    """Check if NVENC is available."""
    return _state["has_nvenc"]


def setup_ffmpeg_nvenc() -> bool:
    """
    Check for and setup ffmpeg with NVENC support.
    Downloads a static build if NVENC-capable ffmpeg is not available.
    Supports older drivers by downloading compatible FFmpeg builds.
    
    Returns:
        True if NVENC is available, False otherwise.
    """
    ffmpeg_path = settings.ffmpeg_nvenc_path
    
    # Check if we already have our custom ffmpeg and it works
    if ffmpeg_path.exists():
        try:
            test_cmd = [
                str(ffmpeg_path), "-y", "-f", "lavfi", "-i", "color=c=black:s=256x256", 
                "-t", "0.1", "-c:v", "h264_nvenc", "-f", "null", "-"
            ]
            if subprocess.run(test_cmd, capture_output=True).returncode == 0:
                print(f"✓ Using FFmpeg with NVENC from {ffmpeg_path}")
                _state["has_nvenc"] = True
                return True
        except Exception:
            pass
    
    # Check for NVIDIA GPU
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return False
        driver_version = result.stdout.strip()
        major_version = int(driver_version.split(".")[0])
    except Exception:
        return False
    
    # Download compatible version
    try:
        ffmpeg_path.parent.mkdir(parents=True, exist_ok=True)
        if major_version >= 570:
            url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz"
        else:
            # Compatible with driver 550
            url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/autobuild-2024-07-31-12-50/ffmpeg-N-116475-g43f702a253-linux64-gpl.tar.xz"
            
        print(f"⬇ Downloading compatible FFmpeg for GPU encoding...")
        temp_dir = tempfile.mkdtemp()
        archive = os.path.join(temp_dir, "ffmpeg.tar.xz")
        subprocess.run(["curl", "-sL", "-o", archive, url], check=True)
        subprocess.run(["tar", "-xf", archive, "-C", temp_dir], check=True)
        
        for item in Path(temp_dir).rglob("ffmpeg"):
            if item.is_file() and not item.is_symlink():
                shutil.copy2(item, ffmpeg_path)
                ffmpeg_path.chmod(0o755)
                break
        shutil.rmtree(temp_dir)
        _state["has_nvenc"] = True
        return True
    except Exception as e:
        print(f"⚠ FFmpeg setup failed: {e}")
        return False


def get_ffmpeg_cmd() -> str:
    """Get the appropriate ffmpeg command path."""
    ffmpeg_path = settings.ffmpeg_nvenc_path
    return str(ffmpeg_path) if has_nvenc() and ffmpeg_path.exists() else "ffmpeg"
