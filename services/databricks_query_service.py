"""Databricks SQL query service for Stage 1 and Stage 2 data."""

import sys
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from databricks import sql

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from config.settings import settings
from infrastructure.databricks_client import get_databricks_connection
from services.databricks_mapping_service import databricks_mapping_service


class DatabricksQueryService:
    """Service for querying Stage 1 and Stage 2 inference data from Databricks."""
    
    def __init__(self, connection=None):
        """
        Initialize the query service.
        
        Args:
            connection: Optional Databricks SQL connection. If not provided, creates one.
        """
        self._connection = connection
    
    @property
    def connection(self):
        """Lazy-load the Databricks SQL connection."""
        if self._connection is None:
            self._connection = get_databricks_connection()
        return self._connection
    
    def _reconnect(self):
        """Force reconnection by closing old connection and creating new one."""
        print("🔄 Reconnecting to Databricks...")
        try:
            if self._connection is not None:
                self._connection.close()
        except Exception as e:
            print(f"  (Error closing old connection: {e})")
        
        self._connection = None
        self._connection = get_databricks_connection()
        print("  ✓ Reconnected successfully")
    
    def _execute_with_retry(self, query_func, max_retries=2):
        """
        Execute a query function with automatic reconnect on connection errors.
        
        Args:
            query_func: Function that takes a connection and executes a query
            max_retries: Maximum number of retry attempts (default 2)
            
        Returns:
            Query result from query_func
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return query_func(self.connection)
            except Exception as e:
                last_error = e
                error_msg = str(e).lower()
                exception_type = type(e).__name__
                
                # Check if it's a connection-related error
                # Include RequestError as it typically indicates network/connection issues
                is_connection_error = (
                    exception_type == 'RequestError' or
                    any(keyword in error_msg for keyword in [
                        'connection', 'closed', 'timeout', 'broken pipe', 
                        'session', 'expired', 'invalid session',
                        'error during request', 'request to server'
                    ])
                )
                
                if is_connection_error and attempt < max_retries - 1:
                    print(f"  ⚠️  Connection error detected (attempt {attempt + 1}/{max_retries})")
                    print(f"  Error type: {exception_type}")
                    print(f"  Error: {e}")
                    self._reconnect()
                    # Retry after reconnect
                else:
                    # Not a connection error, or max retries reached
                    raise
        
        # Should not reach here, but just in case
        raise last_error
    
    def get_available_tenants(self, date_str: str) -> List[Tuple[str, str]]:
        """Get list of tenants that have data on the given date."""
        farm_mapping = databricks_mapping_service.get_farm_mapping()
        
        query = f"""
        SELECT DISTINCT farm_id
        FROM {settings.full_stage1_table}
        WHERE DATE(processing_timestamp) = '{date_str}'
          AND farm_id IS NOT NULL
        ORDER BY farm_id
        """
        
        def execute_query(conn):
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                tenant_set = set()
                for row in results:
                    farm_id = row[0]
                    farm_info = farm_mapping.get(farm_id, {})
                    tenant_id = farm_info.get('tenant_id')
                    tenant_name = farm_info.get('tenant_name', 'Unknown')
                    if tenant_id:
                        tenant_set.add((tenant_name, tenant_id))
                
                tenants = sorted(list(tenant_set), key=lambda x: x[0])
                print(f"  ✓ Found {len(tenants)} tenants")
                return [("All", "All")] + tenants
        
        try:
            return self._execute_with_retry(execute_query)
        except Exception as e:
            print(f"  ✗ ERROR fetching tenants: {e}")
            import traceback
            traceback.print_exc()
            return [("All", "All")]
    
    def get_available_farms(self, date_str: str, tenant_id: Optional[str] = None) -> List[Tuple[str, str]]:
        """
        Get list of farm IDs that have data on the given date, optionally filtered by tenant.
        
        Args:
            date_str: Date in YYYY-MM-DD format.
            tenant_id: Optional tenant ID to filter by.
            
        Returns:
            List of tuples (display_name, farm_id) for dropdown choices.
        """
        farm_mapping = databricks_mapping_service.get_farm_mapping()
        actual_tenant_id = tenant_id[1] if isinstance(tenant_id, tuple) else tenant_id
        
        query = f"""
        SELECT DISTINCT farm_id
        FROM {settings.full_stage1_table}
        WHERE DATE(processing_timestamp) = '{date_str}'
          AND farm_id IS NOT NULL
        ORDER BY farm_id
        LIMIT 100
        """
        
        print(f"")
        print(f"=" * 50)
        print(f"QUERY: get_available_farms")
        print(f"=" * 50)
        print(f"  Date: {date_str}")
        print(f"  Table: {settings.full_stage1_table}")
        
        def execute_query(conn):
            print(f"  Executing query...")
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                print(f"  ✓ SUCCESS: Fetched {len(results)} farms")
                
                farms = []
                for row in results:
                    farm_id = row[0]
                    farm_info = farm_mapping.get(farm_id, {})
                    farm_name = farm_info.get('name', farm_id)
                    
                    if actual_tenant_id and actual_tenant_id != "All":
                        if farm_info.get('tenant_id') != actual_tenant_id:
                            continue
                    
                    farms.append((farm_name, farm_id))
                
                farms.sort(key=lambda x: x[0])
                return [("All", "All")] + farms
        
        try:
            return self._execute_with_retry(execute_query)
        except Exception as e:
            print(f"  ✗ ERROR fetching farms!")
            print(f"  Error type: {type(e).__name__}")
            print(f"  Error message: {str(e)}")
            import traceback
            traceback.print_exc()
            print(f"=" * 50)
            return [("All", "All")]
    
    def get_available_cameras(
        self, 
        date_str: str, 
        farm_id: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        """
        Get list of camera IDs that have data on the given date, optionally filtered by farm.
        
        Args:
            date_str: Date in YYYY-MM-DD format.
            farm_id: Optional farm ID to filter by.
            
        Returns:
            List of tuples (display_name, camera_id) for dropdown choices.
        """
        camera_mapping = databricks_mapping_service.get_camera_mapping()
        
        # Extract actual farm_id from tuple if needed
        actual_farm_id = farm_id[1] if isinstance(farm_id, tuple) else farm_id
        
        farm_filter = f"AND farm_id = '{actual_farm_id}'" if actual_farm_id and actual_farm_id != "All" else ""
        
        query = f"""
        SELECT DISTINCT camera_id
        FROM {settings.full_stage1_table}
        WHERE DATE(processing_timestamp) = '{date_str}'
          AND camera_id IS NOT NULL
          {farm_filter}
        ORDER BY camera_id
        LIMIT 100
        """
        
        def execute_query(conn):
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                cameras = []
                for row in results:
                    camera_id = row[0]
                    camera_info = camera_mapping.get(camera_id, {})
                    camera_name = camera_info.get('name', camera_id)
                    cameras.append((camera_name, camera_id))
                
                cameras.sort(key=lambda x: x[0])
                return [("All", "All")] + cameras
        
        try:
            return self._execute_with_retry(execute_query)
        except Exception as e:
            print(f"Error fetching cameras: {e}")
            import traceback
            traceback.print_exc()
            return [("All", "All")]
    
    def query_stage1_stage2_linked(
        self,
        date_str: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        tenant_id: Optional[str] = None,
        farm_id: Optional[str] = None,
        camera_id: Optional[str] = None,
        should_forward_only: bool = False,
        limit: int = 50
    ) -> pd.DataFrame:
        """
        Query Stage 1 and Stage 2 results with LEFT JOIN.
        
        Returns linked results where Stage 1 is always present,
        and Stage 2 may be NULL for events that weren't forwarded.
        
        Args:
            date_str: Date in YYYY-MM-DD format.
            start_time: Optional start time filter (HH:MM or HH:MM:SS).
            end_time: Optional end time filter (HH:MM or HH:MM:SS).
            farm_id: Optional farm ID filter.
            camera_id: Optional camera ID filter.
            should_forward_only: If True, only return forwarded events.
            limit: Maximum number of results.
            
        Returns:
            DataFrame with linked Stage 1 and Stage 2 results.
        """
        # Build dynamic filters
        filters = []
        
        # Add tenant filter (filter farms by tenant_id)
        if tenant_id and tenant_id != "All":
            farm_mapping = databricks_mapping_service.get_farm_mapping()
            tenant_farm_ids = [
                fid for fid, finfo in farm_mapping.items()
                if finfo.get('tenant_id') == tenant_id
            ]
            if tenant_farm_ids:
                farm_ids_str = "', '".join(tenant_farm_ids)
                filters.append(f"s1.farm_id IN ('{farm_ids_str}')")
            else:
                filters.append("1=0")
        
        # Add time range filter using HOUR and MINUTE extraction
        if start_time:
            st = start_time if start_time.count(':') == 2 else start_time + ":00"
            # Convert to comparable time string format (HH:mm:ss)
            filters.append(f"DATE_FORMAT(s1.stage1_timestamp, 'HH:mm:ss') >= '{st}'")
        
        if end_time:
            et = end_time if end_time.count(':') == 2 else end_time + ":59"
            filters.append(f"DATE_FORMAT(s1.stage1_timestamp, 'HH:mm:ss') <= '{et}'")
        
        if farm_id and farm_id != "All":
            filters.append(f"s1.farm_id = '{farm_id}'")
        
        if camera_id and camera_id != "All":
            filters.append(f"s1.camera_id = '{camera_id}'")
        
        if should_forward_only:
            filters.append("s1.stage1_should_forward = true")
        
        where_clause = " AND ".join(filters) if filters else "1=1"
        
        # Databricks SQL syntax (similar to BigQuery but with some differences)
        query = f"""
        WITH stage1_data AS (
          SELECT 
            session_id,
            farm_id,
            camera_id,
            processing_timestamp AS stage1_timestamp,
            highest_probability_category AS stage1_category,
            highest_probability_value AS stage1_confidence,
            should_forward AS stage1_should_forward,
            frame_uris,
            frame_uris[0] AS trigger_frame_uri,
            -- Extract linkage keys from trigger frame
            -- blk_file = block number + frame offset (e.g., 042_0000015)
            REGEXP_EXTRACT(frame_uris[0], '/(\\\\d{{3}}_\\\\d{{7}})_', 1) AS blk_file,
            REGEXP_EXTRACT(frame_uris[0], '_(\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}}T\\\\d{{2}}:\\\\d{{2}}:\\\\d{{2}})', 1) AS frame_timestamp_key,
            probability_animal_husbandry,
            probability_down_cow,
            probability_quick_movements,
            probability_no_event,
            -- Stage 1 raw response from Gemini (already a string)
            gemini_raw_response AS stage1_raw_response
          FROM {settings.full_stage1_table}
          WHERE DATE(processing_timestamp) = '{date_str}'
        ),
        
        stage2_data AS (
          SELECT 
            inference_id AS stage2_inference_id,
            camera_id,
            inference_timestamp AS stage2_timestamp,
            classification AS stage2_classification,
            max_probability_score AS stage2_confidence,
            should_forward AS stage2_should_forward,
            video_gcs_path,
            file_name AS video_filename,
            -- Extract linkage keys from video filename
            -- blk_file = block number + frame offset (e.g., 042_0000015)
            REGEXP_EXTRACT(file_name, '^(\\\\d{{3}}_\\\\d{{7}})_', 1) AS blk_file,
            REGEXP_EXTRACT(file_name, '_(\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}}T\\\\d{{2}}:\\\\d{{2}}:\\\\d{{2}})', 1) AS video_timestamp_key,
            -- Stage 2 raw response - model_votes is a string, not array
            model_votes AS stage2_raw_response
          FROM {settings.full_stage2_table}
          WHERE DATE(inference_timestamp) BETWEEN DATE_SUB('{date_str}', 2) 
                                              AND DATE_ADD('{date_str}', 2)
        )
        
        SELECT 
          -- Stage 1 Info
          s1.session_id,
          s1.farm_id,
          s1.camera_id,
          s1.stage1_timestamp,
          s1.stage1_category,
          s1.stage1_confidence,
          s1.stage1_should_forward,
          s1.frame_uris,
          s1.trigger_frame_uri,
          SIZE(s1.frame_uris) AS frame_count,
          s1.probability_animal_husbandry,
          s1.probability_down_cow,
          s1.probability_quick_movements,
          s1.probability_no_event,
          s1.stage1_raw_response,
          
          -- Stage 2 Info (may be NULL)
          s2.stage2_inference_id,
          s2.stage2_timestamp,
          s2.stage2_classification,
          s2.stage2_confidence,
          s2.stage2_should_forward,
          s2.video_gcs_path,
          s2.video_filename,
          s2.stage2_raw_response,
          
          -- Linkage keys
          s1.blk_file,
          s1.frame_timestamp_key AS event_timestamp,
          
          -- Derived video path (fallback if Stage 2 missing)
          CASE 
            WHEN s2.video_gcs_path IS NOT NULL THEN s2.video_gcs_path
            ELSE REGEXP_REPLACE(
              REGEXP_REPLACE(s1.trigger_frame_uri, 'frames-to-analyze', 'video-to-analyze'),
              '\\\\.jpg$', '.mp4'
            )
          END AS video_url_derived
          
        FROM stage1_data s1
        LEFT JOIN stage2_data s2
          ON s1.camera_id = s2.camera_id
          AND s1.blk_file = s2.blk_file            -- Match on block number + frame offset
          AND s1.frame_timestamp_key = s2.video_timestamp_key
        
        WHERE {where_clause}
        
        ORDER BY s1.stage1_timestamp DESC
        LIMIT {limit}
        """
        
        print(f"")
        print(f"=" * 50)
        print(f"QUERY: query_stage1_stage2_linked")
        print(f"=" * 50)
        print(f"  Date: {date_str}")
        print(f"  Tenant: {tenant_id}")
        print(f"  Farm: {farm_id}")
        print(f"  Camera: {camera_id}")
        print(f"  Where clause: {where_clause}")
        print(f"  Limit: {limit}")
        
        def execute_query(conn):
            print(f"  Executing complex JOIN query...")
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                # Fetch column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all rows
                rows = cursor.fetchall()
                
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=columns)
                
                print(f"  ✓ SUCCESS: Returned {len(df)} rows")
                print(f"  Columns: {list(df.columns)[:5]}..." if len(df.columns) > 5 else f"  Columns: {list(df.columns)}")
                print(f"=" * 50)
                return df
        
        try:
            return self._execute_with_retry(execute_query)
        except Exception as e:
            print(f"  ✗ ERROR querying data!")
            print(f"  Error type: {type(e).__name__}")
            print(f"  Error message: {str(e)}")
            import traceback
            traceback.print_exc()
            print(f"=" * 50)
            return pd.DataFrame()


# Global instance
databricks_query_service = DatabricksQueryService()
