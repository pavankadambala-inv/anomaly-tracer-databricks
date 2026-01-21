"""BigQuery query service for Stage 1 and Stage 2 data."""

from typing import List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery

from ..config import settings
from ..infrastructure import get_bigquery_client
from .camera_config import camera_config_service


class QueryService:
    """Service for querying Stage 1 and Stage 2 inference data from BigQuery."""
    
    def __init__(self, client: bigquery.Client = None):
        """
        Initialize the query service.
        
        Args:
            client: Optional BigQuery client. If not provided, creates one.
        """
        self._client = client
    
    @property
    def client(self) -> bigquery.Client:
        """Lazy-load the BigQuery client."""
        if self._client is None:
            self._client = get_bigquery_client()
        return self._client
    
    def get_available_farms(self, date_str: str) -> List[Tuple[str, str]]:
        """
        Get list of farm IDs that have data on the given date.
        
        Args:
            date_str: Date in YYYY-MM-DD format.
            
        Returns:
            List of tuples (display_name, farm_id) for dropdown choices.
        """
        farm_mapping = camera_config_service.get_farm_mapping()
        
        query = f"""
        SELECT DISTINCT farm_id
        FROM `{settings.project_id}.{settings.dataset_id}.{settings.stage1_table}`
        WHERE DATE(processing_timestamp) = @target_date
          AND farm_id IS NOT NULL
        ORDER BY farm_id
        LIMIT 100
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_date", "DATE", date_str)
            ]
        )
        
        try:
            results = self.client.query(query, job_config=job_config).result()
            farms = []
            for row in results:
                farm_id = row.farm_id
                farm_name = farm_mapping.get(farm_id, farm_id)
                farms.append((farm_name, farm_id))
            # Sort by display name
            farms.sort(key=lambda x: x[0])
            return [("All", "All")] + farms
        except Exception as e:
            print(f"Error fetching farms: {e}")
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
        camera_mapping = camera_config_service.get_camera_mapping()
        
        # Extract actual farm_id from tuple if needed
        actual_farm_id = farm_id[1] if isinstance(farm_id, tuple) else farm_id
        
        farm_filter = "AND farm_id = @farm_id" if actual_farm_id and actual_farm_id != "All" else ""
        
        query = f"""
        SELECT DISTINCT camera_id
        FROM `{settings.project_id}.{settings.dataset_id}.{settings.stage1_table}`
        WHERE DATE(processing_timestamp) = @target_date
          AND camera_id IS NOT NULL
          {farm_filter}
        ORDER BY camera_id
        LIMIT 100
        """
        
        params = [bigquery.ScalarQueryParameter("target_date", "DATE", date_str)]
        if actual_farm_id and actual_farm_id != "All":
            params.append(bigquery.ScalarQueryParameter("farm_id", "STRING", actual_farm_id))
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        
        try:
            results = self.client.query(query, job_config=job_config).result()
            cameras = []
            for row in results:
                camera_id = row.camera_id
                camera_info = camera_mapping.get(camera_id, {})
                camera_name = camera_info.get('name', camera_id)
                cameras.append((camera_name, camera_id))
            # Sort by display name
            cameras.sort(key=lambda x: x[0])
            return [("All", "All")] + cameras
        except Exception as e:
            print(f"Error fetching cameras: {e}")
            return [("All", "All")]
    
    def query_stage1_stage2_linked(
        self,
        date_str: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
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
        params = [bigquery.ScalarQueryParameter("target_date", "DATE", date_str)]
        
        # Add time range filter
        if start_time:
            filters.append("TIME(s1.stage1_timestamp) >= PARSE_TIME('%H:%M:%S', @start_time)")
            st = start_time if start_time.count(':') == 2 else start_time + ":00"
            params.append(bigquery.ScalarQueryParameter("start_time", "STRING", st))
        
        if end_time:
            filters.append("TIME(s1.stage1_timestamp) <= PARSE_TIME('%H:%M:%S', @end_time)")
            et = end_time if end_time.count(':') == 2 else end_time + ":59"
            params.append(bigquery.ScalarQueryParameter("end_time", "STRING", et))
        
        if farm_id and farm_id != "All":
            filters.append("s1.farm_id = @farm_id")
            params.append(bigquery.ScalarQueryParameter("farm_id", "STRING", farm_id))
        
        if camera_id and camera_id != "All":
            filters.append("s1.camera_id = @camera_id")
            params.append(bigquery.ScalarQueryParameter("camera_id", "STRING", camera_id))
        
        if should_forward_only:
            filters.append("s1.stage1_should_forward = true")
        
        where_clause = " AND ".join(filters) if filters else "1=1"
        
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
            frame_uris[SAFE_OFFSET(0)] AS trigger_frame_uri,
            -- Extract linkage keys from trigger frame
            -- blk_file = block number + frame offset (e.g., 042_0000015)
            REGEXP_EXTRACT(frame_uris[SAFE_OFFSET(0)], r"/(\\d{{3}}_\\d{{7}})_") AS blk_file,
            REGEXP_EXTRACT(frame_uris[SAFE_OFFSET(0)], r"_(\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}})") AS frame_timestamp_key,
            probability_animal_husbandry,
            probability_down_cow,
            probability_quick_movements,
            probability_no_event,
            -- Stage 1 raw response from Gemini
            TO_JSON_STRING(gemini_raw_response) AS stage1_raw_response
          FROM `{settings.project_id}.{settings.dataset_id}.{settings.stage1_table}`
          WHERE DATE(processing_timestamp) = @target_date
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
            REGEXP_EXTRACT(file_name, r"^(\\d{{3}}_\\d{{7}})_") AS blk_file,
            REGEXP_EXTRACT(file_name, r"_(\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}})") AS video_timestamp_key,
            -- Stage 2 raw response from first model vote
            model_votes[SAFE_OFFSET(0)].raw_response AS stage2_raw_response
          FROM `{settings.project_id}.{settings.dataset_id}.{settings.stage2_table}`
          WHERE DATE(inference_timestamp) BETWEEN DATE_SUB(@target_date, INTERVAL 2 DAY) 
                                              AND DATE_ADD(@target_date, INTERVAL 2 DAY)
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
          ARRAY_LENGTH(s1.frame_uris) AS frame_count,
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
              REGEXP_REPLACE(s1.trigger_frame_uri, r'frames-to-analyze', 'video-to-analyze'),
              r'\\.jpg$', '.mp4'
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
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        
        print(f"DEBUG query_stage1_stage2_linked: date={date_str}, farm={farm_id}, camera={camera_id}")
        print(f"DEBUG query_stage1_stage2_linked: where_clause={where_clause}")
        
        try:
            results = self.client.query(query, job_config=job_config).result()
            df = results.to_dataframe()
            print(f"DEBUG query_stage1_stage2_linked: returned {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error querying data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()


# Global instance
query_service = QueryService()
