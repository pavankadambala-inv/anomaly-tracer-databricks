"""Event handlers for Gradio UI interactions."""

import json
import sys
from pathlib import Path
from typing import Any, Optional, Tuple

import gradio as gr
import pandas as pd

# Ensure parent directory is in path
_parent = Path(__file__).resolve().parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from services import query_service, media_service
from services.databricks_mapping_service import databricks_mapping_service
from ui.formatters import format_results_for_display
from ui.state import app_state


def _extract_dropdown_value(value: Any) -> Optional[str]:
    """
    Extract the actual string value from a Gradio dropdown.
    
    Handles both string values and tuple values (display_name, actual_value).
    Returns None if the value is "All" or empty.
    """
    if value is None:
        return None
    
    # If it's a tuple, extract the second element (the actual value)
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        actual = value[1]
    else:
        actual = value
    
    # Convert to string and check for "All"
    actual_str = str(actual) if actual is not None else None
    
    if actual_str in (None, "", "All"):
        return None
    
    return actual_str


def load_filters(date_str: str) -> Tuple[gr.Dropdown, gr.Dropdown, gr.Dropdown, str]:
    """
    Load available tenants, farms, and cameras for a given date.
    
    Args:
        date_str: Date in YYYY-MM-DD format.
        
    Returns:
        Tuple of (tenants_dropdown, farms_dropdown, cameras_dropdown, status_message)
    """
    tenants = query_service.get_available_tenants(date_str)
    farms = query_service.get_available_farms(date_str)
    cameras = query_service.get_available_cameras(date_str)
    return (
        gr.Dropdown(choices=tenants, value="All"),
        gr.Dropdown(choices=farms, value="All"),
        gr.Dropdown(choices=cameras, value="All"),
        f"Loaded {len(tenants)-1} tenants, {len(farms)-1} farms, {len(cameras)-1} cameras for {date_str}"
    )


def update_farms_on_tenant_change(date_str: str, tenant_id: str) -> Tuple[gr.Dropdown, gr.Dropdown]:
    """Update farm and camera dropdowns when tenant selection changes."""
    actual_tenant_id = _extract_dropdown_value(tenant_id)
    farms = query_service.get_available_farms(date_str, actual_tenant_id)
    cameras = query_service.get_available_cameras(date_str)
    return (
        gr.Dropdown(choices=farms, value="All"),
        gr.Dropdown(choices=cameras, value="All"),
    )


def update_cameras_on_farm_change(date_str: str, farm_id: str) -> gr.Dropdown:
    """
    Update camera dropdown when farm selection changes.
    
    Args:
        date_str: Date in YYYY-MM-DD format.
        farm_id: Selected farm ID.
        
    Returns:
        Updated cameras dropdown.
    """
    actual_farm_id = _extract_dropdown_value(farm_id)
    cameras = query_service.get_available_cameras(date_str, actual_farm_id)
    return gr.Dropdown(choices=cameras, value="All")


def run_query(
    date_str: str,
    start_time: str,
    end_time: str,
    tenant_id: str,
    farm_id: str,
    camera_id: str,
    should_forward_only: bool
) -> Tuple[pd.DataFrame, str]:
    """
    Run the query and return formatted results.
    
    Args:
        date_str: Date in YYYY-MM-DD format.
        start_time: Optional start time filter.
        end_time: Optional end time filter.
        farm_id: Optional farm ID filter.
        camera_id: Optional camera ID filter.
        should_forward_only: If True, only return forwarded events.
        
    Returns:
        Tuple of (formatted_dataframe, status_message)
    """
    camera_mapping = databricks_mapping_service.get_camera_mapping()
    farm_mapping = databricks_mapping_service.get_farm_mapping()
    
    actual_tenant_id = _extract_dropdown_value(tenant_id)
    actual_farm_id = _extract_dropdown_value(farm_id)
    actual_camera_id = _extract_dropdown_value(camera_id)
    
    print(f"DEBUG run_query: tenant_id={tenant_id!r} -> {actual_tenant_id!r}")
    print(f"DEBUG run_query: farm_id={farm_id!r} -> {actual_farm_id!r}")
    print(f"DEBUG run_query: camera_id={camera_id!r} -> {actual_camera_id!r}")
    
    try:
        df = query_service.query_stage1_stage2_linked(
            date_str=date_str,
            start_time=start_time.strip() if start_time.strip() else None,
            end_time=end_time.strip() if end_time.strip() else None,
            tenant_id=actual_tenant_id,
            farm_id=actual_farm_id,
            camera_id=actual_camera_id,
            should_forward_only=should_forward_only,
            limit=100
        )
        
        # Store in app state for row selection
        app_state.query_results = df
        
        filter_parts = [f"Date: {date_str}"]
        if start_time.strip():
            filter_parts.append(f"From: {start_time}")
        if end_time.strip():
            filter_parts.append(f"To: {end_time}")
        if actual_tenant_id:
            tenant_display = databricks_mapping_service.get_tenant_display_name(actual_tenant_id)
            filter_parts.append(f"Tenant: {tenant_display}")
        if actual_farm_id:
            farm_info = farm_mapping.get(actual_farm_id, {})
            farm_display = farm_info.get('name', actual_farm_id)
            filter_parts.append(f"Farm: {farm_display}")
        if actual_camera_id:
            camera_info = camera_mapping.get(actual_camera_id, {})
            camera_display = camera_info.get('name', actual_camera_id)
            filter_parts.append(f"Camera: {camera_display}")
        filter_summary = " | ".join(filter_parts)
        
        if df.empty:
            return pd.DataFrame(), f"No results found. Filters: {filter_summary}"
        
        # Clear row cache when new query results are loaded
        app_state.row_cache.clear()
        app_state.last_selected_row = None
        
        display_df = format_results_for_display(df)
        print(f"DEBUG run_query: display_df shape={display_df.shape}, columns={list(display_df.columns)}")
        status = f"Found {len(df)} results | {filter_summary}"
        
        return display_df, status
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), f"Error: {str(e)}"


def get_row_details(evt: gr.SelectData) -> Tuple[Optional[str], Optional[str], str]:
    """
    Get frame GIF and video details for selected row.
    
    Args:
        evt: Gradio select event containing row index.
        
    Returns:
        Tuple of (gif_path, video_path, details_text)
    """
    if app_state.query_results.empty:
        return None, None, "No data available"
    
    try:
        row_idx = evt.index[0]
        
        # Check if this is the same row being selected again (prevent redundant downloads)
        if row_idx == app_state.last_selected_row and row_idx in app_state.row_cache:
            print(f"Using cached data for row {row_idx}")
            return app_state.row_cache[row_idx]
        
        # Update last selected row
        app_state.last_selected_row = row_idx
        
        row = app_state.query_results.iloc[row_idx]
        
        # Get camera and farm display names
        camera_id = row.get('camera_id', '')
        farm_id = row.get('farm_id', '')
        camera_info = databricks_mapping_service.get_camera_info(camera_id) if camera_id else {'name': 'N/A'}
        farm_name = databricks_mapping_service.get_farm_display_name(farm_id) if farm_id else 'N/A'
        
        # Build details text (plain text format)
        details = []
        details.append(f"Session ID: {row.get('session_id', 'N/A')}")
        details.append("")
        details.append("═══ Location ═══")
        details.append(f"  Farm: {farm_name}")
        details.append(f"  Farm ID: {farm_id or 'N/A'}")
        details.append(f"  Camera: {camera_info['name']}")
        details.append(f"  Camera ID: {camera_id or 'N/A'}")
        details.append("")
        details.append("═══ Stage 1 Results ═══")
        details.append(f"  Category: {row.get('stage1_category', 'N/A')}")
        details.append(f"  Confidence: {row.get('stage1_confidence', 'N/A'):.3f}" if pd.notna(row.get('stage1_confidence')) else "  Confidence: N/A")
        s1_forward = row.get('stage1_should_forward')
        details.append(f"  Should Forward: {'N/A' if pd.isna(s1_forward) else ('Yes ✓' if s1_forward else 'No ✗')}")
        details.append(f"  Frame Count: {row.get('frame_count', 'N/A')}")
        details.append(f"  Timestamp: {row.get('stage1_timestamp', 'N/A')}")
        details.append("")
        
        if pd.notna(row.get('stage2_inference_id')):
            details.append("═══ Stage 2 Results ═══")
            details.append(f"  Classification: {row.get('stage2_classification', 'N/A')}")
            details.append(f"  Confidence: {row.get('stage2_confidence', 'N/A'):.3f}" if pd.notna(row.get('stage2_confidence')) else "  Confidence: N/A")
            s2_forward = row.get('stage2_should_forward')
            details.append(f"  Should Forward: {'N/A' if pd.isna(s2_forward) else ('Yes ✓' if s2_forward else 'No ✗')}")
        else:
            details.append("═══ Stage 2 Results ═══")
            details.append("  (No Stage 2 processing - event not forwarded)")
        
        # Add raw responses section
        details.append("")
        details.append("═══ Stage 1 Raw Response ═══")
        s1_raw = row.get('stage1_raw_response')
        if pd.notna(s1_raw) and s1_raw:
            try:
                # The raw response might contain literal \n characters that need to be unescaped
                # First, if it's a string with escaped newlines, unescape them
                if isinstance(s1_raw, str) and '\\n' in s1_raw:
                    # Replace literal \n with actual newlines
                    s1_raw = s1_raw.replace('\\n', '\n')
                
                # Parse and reformat JSON
                s1_json = json.loads(s1_raw)
                formatted = json.dumps(s1_json, indent=2)
                details.append(formatted)
            except (json.JSONDecodeError, TypeError):
                # If JSON parsing fails, just clean up the escaped characters
                cleaned = str(s1_raw).replace('\\n', '\n').replace('\\"', '"')[:2000]
                details.append(cleaned)
        else:
            details.append("  (No raw response available)")
        
        details.append("")
        details.append("═══ Stage 2 Raw Response ═══")
        s2_raw = row.get('stage2_raw_response')
        if pd.notna(s2_raw) and s2_raw:
            try:
                # The raw response might contain literal \n characters that need to be unescaped
                # First, if it's a string with escaped newlines, unescape them
                if isinstance(s2_raw, str) and '\\n' in s2_raw:
                    # Replace literal \n with actual newlines
                    s2_raw = s2_raw.replace('\\n', '\n')
                
                # Parse and reformat JSON
                s2_json = json.loads(s2_raw)
                formatted = json.dumps(s2_json, indent=2)
                details.append(formatted)
            except (json.JSONDecodeError, TypeError):
                # If JSON parsing fails, just clean up the escaped characters
                cleaned = str(s2_raw).replace('\\n', '\n').replace('\\"', '"')[:2000]
                details.append(cleaned)
        else:
            details.append("  (No raw response available)")
        
        details_text = "\n".join(details)
        
        # Create animated GIF from all Stage 1 frames
        frame_uris = row.get('frame_uris')
        gif_path = None
        # Handle numpy arrays, lists, or None - avoid ambiguous truth check
        if frame_uris is not None and len(frame_uris) > 0:
            # Convert to list if it's a numpy array
            frame_list = list(frame_uris) if hasattr(frame_uris, '__iter__') else []
            if frame_list:
                gif_path = media_service.create_animated_gif_from_frames(frame_list, fps=3)
        
        # Download video only if Stage 2 exists (has actual video)
        video_path = None
        stage2_id = row.get('stage2_inference_id')
        video_gcs = row.get('video_gcs_path')
        print(f"DEBUG: stage2_inference_id={stage2_id}, video_gcs_path={video_gcs}")
        if pd.notna(stage2_id) and pd.notna(video_gcs):
            video_path = media_service.download_video_to_temp(video_gcs)
        
        # Cache the result
        result = (gif_path, video_path, details_text)
        app_state.row_cache[row_idx] = result
        
        return result
        
    except Exception as e:
        return None, None, f"Error loading details: {str(e)}"
