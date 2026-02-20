"""Gradio UI components and layout."""

from datetime import datetime

import gradio as gr

from ui.handlers import run_query, get_row_details, load_filters, update_cameras_on_farm_change, update_farms_on_tenant_change


def create_app() -> gr.Blocks:
    """
    Create and configure the Gradio application.
    
    Returns:
        Configured Gradio Blocks application.
    """
    # Get today's date as default
    today = datetime.now().strftime("%Y-%m-%d")
    
    with gr.Blocks(title="CV Inference Traceability Dashboard") as app:
        
        gr.Markdown("""
        # 🐄 CV Inference Traceability Dashboard
        
        Query and visualize **Stage 1** (frame detection) and **Stage 2** (video classification) 
        inference results from BigQuery. Click on a row to view the trigger frame and video.
        """)
        
        # =====================================================================
        # Filters Section
        # =====================================================================
        with gr.Row():
            with gr.Column(scale=1):
                date_picker = gr.Textbox(
                    label="📅 Date (YYYY-MM-DD)",
                    value=today,
                    placeholder="2026-01-14"
                )
            with gr.Column(scale=1):
                start_time = gr.Textbox(
                    label="🕐 Start Time (HH:MM)",
                    value="",
                    placeholder="e.g. 08:00 (leave empty for all)"
                )
            with gr.Column(scale=1):
                end_time = gr.Textbox(
                    label="🕐 End Time (HH:MM)",
                    value="",
                    placeholder="e.g. 17:00 (leave empty for all)"
                )
        
        with gr.Row():
            with gr.Column(scale=1):
                tenant_dropdown = gr.Dropdown(
                    label="🏢 Tenant",
                    choices=["All"],
                    value="All",
                    interactive=True
                )
            with gr.Column(scale=1):
                farm_dropdown = gr.Dropdown(
                    label="🏠 Farm",
                    choices=["All"],
                    value="All",
                    interactive=True
                )
            with gr.Column(scale=1):
                camera_dropdown = gr.Dropdown(
                    label="📷 Camera",
                    choices=["All"],
                    value="All",
                    interactive=True
                )
        
        with gr.Row():
            with gr.Column(scale=1):
                forward_only = gr.Checkbox(
                    label="📤 Only Forwarded (should_forward=true)",
                    value=False
                )
        
        with gr.Row():
            load_filters_btn = gr.Button("🔄 Load Farms/Cameras", variant="secondary")
            query_btn = gr.Button("🔍 Run Query", variant="primary")
        
        status_text = gr.Textbox(label="Status", interactive=False, max_lines=1)
        
        # =====================================================================
        # Results Section
        # =====================================================================
        gr.Markdown("## 📊 Query Results")
        gr.Markdown("*Click on a row to view the trigger frame and video*")
        
        results_table = gr.Dataframe(
            headers=["Farm", "Camera", "Stage 1 Time", "Event Timestamp",
                     "S1 Category", "S1 Conf", "S1 Forward", 
                     "S2 Class", "S2 Conf", "S2 Forward"],
            interactive=False,
            wrap=True,
            elem_classes=["results-table"]
        )
        
        # =====================================================================
        # Media Display Section
        # =====================================================================
        gr.Markdown("## 🎬 Media & Details")
        
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### 📸 Stage 1 Frames (Animated)")
                frame_display = gr.Image(
                    label=None,
                    show_label=False,
                    type="filepath",
                    height=400,
                    interactive=False
                )
            
            with gr.Column(scale=1):
                gr.Markdown("### 🎥 Stage 2 Video")
                video_display = gr.Video(
                    label=None,
                    show_label=False,
                    height=400,
                    autoplay=True,
                    include_audio=True,
                    interactive=False
                )
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("### 📋 Details")
                details_display = gr.Textbox(
                    value="Select a row from the results table to view details",
                    label=None,
                    show_label=False,
                    lines=25,
                    max_lines=50,
                    interactive=False,
                    elem_id="details-box"
                )
        
        # =====================================================================
        # Event Handlers
        # =====================================================================
        
        # Load filters button
        load_filters_btn.click(
            fn=load_filters,
            inputs=[date_picker],
            outputs=[tenant_dropdown, farm_dropdown, camera_dropdown, status_text]
        )
        
        # Update farms when tenant changes
        tenant_dropdown.change(
            fn=update_farms_on_tenant_change,
            inputs=[date_picker, tenant_dropdown],
            outputs=[farm_dropdown, camera_dropdown]
        )
        
        # Update cameras when farm changes
        farm_dropdown.change(
            fn=update_cameras_on_farm_change,
            inputs=[date_picker, farm_dropdown],
            outputs=[camera_dropdown]
        )
        
        # Run query button
        query_btn.click(
            fn=run_query,
            inputs=[date_picker, start_time, end_time, tenant_dropdown, farm_dropdown, camera_dropdown, 
                    forward_only],
            outputs=[results_table, status_text]
        )
        
        # Row selection - show frame and video
        results_table.select(
            fn=get_row_details,
            outputs=[frame_display, video_display, details_display]
        )
        
        # =====================================================================
        # Footer
        # =====================================================================
        gr.Markdown("""
        ---
        ### ℹ️ About
        
        This dashboard queries the following BigQuery tables:
        - **Stage 1**: `invisible-animal-welfare.cv_logs.gemini_stage1_detections`
        - **Stage 2**: `invisible-animal-welfare.cv_logs.stage2_vlm_inferences`
        
        Results are linked using the composite key: `(camera_id, blk_file, timestamp)` where `blk_file` = block number + frame offset (e.g., 042_0000015)
        
        **Note**: Stage 2 columns show "N/A" for events that weren't forwarded for video analysis.
        """)
    
    try:
        app.theme = gr.themes.Soft(primary_hue="blue", secondary_hue="gray")
    except Exception:
        pass
    
    try:
        app.css = """
        .results-table { font-size: 12px; }
        #details-box textarea,
        #details-box .wrap textarea,
        #details-box > div > textarea {
            background: transparent !important;
            color: #ffffff !important;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            padding: 16px !important;
            border: none !important;
            line-height: 1.6 !important;
        }
        """
    except Exception:
        pass
    
    return app
