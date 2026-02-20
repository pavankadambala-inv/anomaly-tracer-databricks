"""Databricks table-based mapping service for cameras, farms, and tenants."""

from typing import Dict, Tuple, Optional
from infrastructure.databricks_client import get_databricks_connection
from config.settings import settings


class DatabricksMappingService:
    """Service for loading camera/farm/tenant mappings from Databricks tables."""
    
    MAPPING_SCHEMA = "cv_logs"
    
    def __init__(self):
        """Initialize the mapping service."""
        self._camera_mapping: Dict[str, Dict[str, str]] = {}
        self._farm_mapping: Dict[str, Dict[str, str]] = {}
        self._tenant_mapping: Dict[str, Dict[str, str]] = {}
        self._loaded = False
    
    def load(self) -> Tuple[Dict, Dict, Dict]:
        """
        Load mappings from Databricks tables.
        
        Returns:
            Tuple of (camera_mapping, farm_mapping, tenant_mapping)
        """
        if self._loaded:
            return self._camera_mapping, self._farm_mapping, self._tenant_mapping
        
        camera_mapping = {}
        farm_mapping = {}
        tenant_mapping = {}
        
        try:
            conn = get_databricks_connection()
            
            print("Loading tenant mappings from Databricks...")
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT tenant_id, tenant_name, tenant_ui_url, tenant_slug
                    FROM {settings.catalog_name}.{self.MAPPING_SCHEMA}.tenant_map
                    WHERE tenant_id IS NOT NULL
                      AND tenant_id != 'tenant_id'
                """)
                
                for row in cursor.fetchall():
                    tenant_id, tenant_name, tenant_ui_url, tenant_slug = row
                    tenant_mapping[tenant_id] = {
                        'name': tenant_name or 'Unknown Tenant',
                        'ui_url': tenant_ui_url or '',
                        'slug': tenant_slug or ''
                    }
            
            print(f"  ✓ Loaded {len(tenant_mapping)} tenants")
            
            print("Loading farm mappings from Databricks...")
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT farm_id, farm_name, tenant_id
                    FROM {settings.catalog_name}.{self.MAPPING_SCHEMA}.farm_map
                    WHERE farm_id IS NOT NULL
                      AND farm_id != 'farm_id'
                """)
                
                for row in cursor.fetchall():
                    farm_id, farm_name, tenant_id = row
                    tenant_name = tenant_mapping.get(tenant_id, {}).get('name', 'Unknown Tenant') if tenant_id else 'Unknown Tenant'
                    
                    farm_mapping[farm_id] = {
                        'name': farm_name or 'Unknown Farm',
                        'tenant_id': tenant_id or '',
                        'tenant_name': tenant_name
                    }
            
            print(f"  ✓ Loaded {len(farm_mapping)} farms")
            
            print("Loading camera mappings from Databricks...")
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT camera_id, camera_name
                    FROM {settings.catalog_name}.{self.MAPPING_SCHEMA}.farm_camera_map
                    WHERE camera_id IS NOT NULL
                      AND camera_id != 'camera_id'
                """)
                
                for row in cursor.fetchall():
                    camera_id, camera_name = row
                    camera_mapping[camera_id] = {
                        'name': camera_name or 'Unknown Camera'
                    }
            
            print(f"  ✓ Loaded {len(camera_mapping)} cameras")
            
            conn.close()
            
        except Exception as e:
            print(f"Warning: Error loading mappings from Databricks: {e}")
            import traceback
            traceback.print_exc()
            print("  Continuing with empty mappings...")
        
        self._camera_mapping = camera_mapping
        self._farm_mapping = farm_mapping
        self._tenant_mapping = tenant_mapping
        self._loaded = True
        
        return camera_mapping, farm_mapping, tenant_mapping
    
    def reload(self):
        """Force reload of mappings from Databricks."""
        self._loaded = False
        return self.load()
    
    def get_camera_mapping(self) -> Dict[str, Dict[str, str]]:
        if not self._loaded:
            self.load()
        return self._camera_mapping
    
    def get_farm_mapping(self) -> Dict[str, Dict[str, str]]:
        if not self._loaded:
            self.load()
        return self._farm_mapping
    
    def get_tenant_mapping(self) -> Dict[str, Dict[str, str]]:
        if not self._loaded:
            self.load()
        return self._tenant_mapping
    
    def get_camera_display_name(self, camera_id: str) -> str:
        mapping = self.get_camera_mapping()
        if camera_id in mapping:
            return mapping[camera_id]['name']
        return camera_id
    
    def get_farm_display_name(self, farm_id: str) -> str:
        mapping = self.get_farm_mapping()
        if farm_id in mapping:
            return mapping[farm_id]['name']
        return farm_id
    
    def get_tenant_display_name(self, tenant_id: str) -> str:
        mapping = self.get_tenant_mapping()
        if tenant_id in mapping:
            return mapping[tenant_id]['name']
        return tenant_id
    
    def get_camera_info(self, camera_id: str) -> Dict[str, str]:
        mapping = self.get_camera_mapping()
        if camera_id in mapping:
            return mapping[camera_id]
        return {'name': camera_id}
    
    def get_farm_info(self, farm_id: str) -> Dict[str, str]:
        mapping = self.get_farm_mapping()
        if farm_id in mapping:
            return mapping[farm_id]
        return {'name': farm_id, 'tenant_id': '', 'tenant_name': 'Unknown'}


databricks_mapping_service = DatabricksMappingService()
