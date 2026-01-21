"""Data formatting utilities for display in the UI."""

import pandas as pd

from ..services import camera_config_service


def format_results_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format DataFrame for display in Gradio table.
    
    Args:
        df: Raw DataFrame from query results.
        
    Returns:
        Formatted DataFrame suitable for display.
    """
    if df.empty:
        return df
    
    # Load camera config for name mapping
    camera_mapping = camera_config_service.get_camera_mapping()
    farm_mapping = camera_config_service.get_farm_mapping()
    
    # Create a copy to work with
    result = df.copy()
    
    # Map farm_id to farm name
    if 'farm_id' in result.columns:
        result['Farm'] = result['farm_id'].apply(
            lambda x: farm_mapping.get(x, x) if pd.notna(x) else "N/A"
        )
    
    # Map camera_id to camera name
    if 'camera_id' in result.columns:
        result['Camera'] = result['camera_id'].apply(
            lambda x: camera_mapping.get(x, {}).get('name', x) if pd.notna(x) else "N/A"
        )
    
    # Select and rename columns for display
    display_cols = {
        'Farm': 'Farm',
        'Camera': 'Camera',
        'stage1_timestamp': 'Stage 1 Time',
        'event_timestamp': 'Event Timestamp',
        'stage1_category': 'S1 Category',
        'stage1_confidence': 'S1 Conf',
        'stage1_should_forward': 'S1 Forward',
        'stage2_classification': 'S2 Class',
        'stage2_confidence': 'S2 Conf',
        'stage2_should_forward': 'S2 Forward'
    }
    
    result = result[[c for c in display_cols.keys() if c in result.columns]].copy()
    result.columns = [display_cols.get(c, c) for c in result.columns]
    
    # Format confidence values
    for col in ['S1 Conf', 'S2 Conf']:
        if col in result.columns:
            result[col] = result[col].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "N/A")
    
    # Format timestamps
    for col in ['Stage 1 Time']:
        if col in result.columns:
            result[col] = result[col].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(x) else "N/A"
            )
    
    # Format booleans
    for col in ['S1 Forward', 'S2 Forward']:
        if col in result.columns:
            result[col] = result[col].apply(lambda x: "N/A" if pd.isna(x) else ("✓" if x else "✗"))
    
    return result
