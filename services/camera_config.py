"""Camera configuration loading and mapping service."""

import sys
from pathlib import Path
from typing import Dict, Tuple

import yaml

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings


class CameraConfigService:
    """Service for loading and querying camera/farm configuration."""
    
    def __init__(self, config_dir: Path = None):
        """
        Initialize the camera config service.
        
        Args:
            config_dir: Directory containing camera config YAML files.
                       Defaults to settings.camera_config_dir.
        """
        self._config_dir = config_dir or settings.camera_config_dir
        self._camera_mapping: Dict[str, Dict[str, str]] = {}
        self._farm_mapping: Dict[str, str] = {}
        self._loaded = False
    
    def load(self) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
        """
        Load camera configuration from YAML files and build mapping dictionaries.
        
        Returns:
            Tuple of (camera_mapping, farm_mapping) where:
            - camera_mapping: {camera_uuid: {'name': camera_name, 'farm_name': farm_name, 'farm_id': farm_uuid}}
            - farm_mapping: {farm_uuid: farm_name}
        """
        # Return cached if already loaded
        if self._loaded:
            return self._camera_mapping, self._farm_mapping
        
        camera_mapping = {}
        farm_mapping = {}
        
        if not self._config_dir or not self._config_dir.exists():
            print(f"Warning: Camera config directory not found: {self._config_dir}")
            self._loaded = True
            return camera_mapping, farm_mapping
        
        # Load all YAML config files
        for config_file in self._config_dir.glob("*.yaml"):
            try:
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                
                if not config or 'farms' not in config:
                    continue
                
                for farm in config.get('farms', []):
                    farm_name = farm.get('name', 'Unknown Farm')
                    farm_uuid = farm.get('uuid', '')
                    
                    if farm_uuid:
                        farm_mapping[farm_uuid] = farm_name
                    
                    for camera in farm.get('cameras', []):
                        camera_uuid = camera.get('uuid', '')
                        camera_name = camera.get('name', 'Unknown Camera')
                        
                        if camera_uuid:
                            camera_mapping[camera_uuid] = {
                                'name': camera_name,
                                'farm_name': farm_name,
                                'farm_id': farm_uuid
                            }
            except Exception as e:
                print(f"Error loading config file {config_file}: {e}")
        
        self._camera_mapping = camera_mapping
        self._farm_mapping = farm_mapping
        self._loaded = True
        
        print(f"✓ Loaded {len(camera_mapping)} cameras from {len(farm_mapping)} farms")
        return camera_mapping, farm_mapping
    
    def get_camera_mapping(self) -> Dict[str, Dict[str, str]]:
        """Get the camera mapping dictionary, loading if necessary."""
        if not self._loaded:
            self.load()
        return self._camera_mapping
    
    def get_farm_mapping(self) -> Dict[str, str]:
        """Get the farm mapping dictionary, loading if necessary."""
        if not self._loaded:
            self.load()
        return self._farm_mapping
    
    def get_camera_display_name(self, camera_id: str) -> str:
        """Get display name for a camera ID (UUID)."""
        camera_mapping = self.get_camera_mapping()
        if camera_id in camera_mapping:
            return camera_mapping[camera_id]['name']
        return camera_id  # Return original ID if not found
    
    def get_farm_display_name(self, farm_id: str) -> str:
        """Get display name for a farm ID (UUID)."""
        farm_mapping = self.get_farm_mapping()
        if farm_id in farm_mapping:
            return farm_mapping[farm_id]
        return farm_id  # Return original ID if not found
    
    def get_camera_info(self, camera_id: str) -> Dict[str, str]:
        """Get full camera info including farm name for a camera ID."""
        camera_mapping = self.get_camera_mapping()
        if camera_id in camera_mapping:
            return camera_mapping[camera_id]
        return {'name': camera_id, 'farm_name': 'Unknown', 'farm_id': ''}


# Global instance
camera_config_service = CameraConfigService()
