# Databricks notebook source
# MAGIC %md
# MAGIC # CV Inference Traceability Dashboard
# MAGIC 
# MAGIC This notebook launches a Gradio web application to query and visualize Stage 1 and Stage 2 
# MAGIC inference results from Databricks Unity Catalog tables.
# MAGIC 
# MAGIC ## Setup Instructions
# MAGIC 
# MAGIC 1. **Install Dependencies**: Run the cell below to install required packages
# MAGIC 2. **Configure Settings**: Update the configuration in the settings cell
# MAGIC 3. **Launch App**: Run the launch cell to start the Gradio interface
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC The app connects to:
# MAGIC - **Stage 1 Table**: `main.cv_logs.gemini_stage1_detections`
# MAGIC - **Stage 2 Table**: `main.cv_logs.stage2_vlm_inferences`
# MAGIC 
# MAGIC Unity Catalog table paths:
# MAGIC - Stage 1: `gs://stg-cv/__unitystorage/catalogs/6e7f553f-6deb-4fde-a5b7-79043ade6dc8/tables/812795be-53d6-4a99-b066-f70cad607e9a`
# MAGIC - Stage 2: `gs://stg-cv/__unitystorage/catalogs/6e7f553f-6deb-4fde-a5b7-79043ade6dc8/tables/3ca3eb93-5e84-4ea0-a92e-a6b9984147b2`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install gradio>=4.0.0 databricks-sql-connector>=3.0.0 databricks-sdk>=0.18.0 google-cloud-storage>=2.10.0 Pillow>=10.0.0 PyYAML>=6.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Upload Application Files
# MAGIC 
# MAGIC Upload the entire application directory to Databricks:
# MAGIC - Option A: Use Databricks Repos (recommended)
# MAGIC - Option B: Upload to DBFS or Workspace
# MAGIC 
# MAGIC The directory structure should be:
# MAGIC ```
# MAGIC anomaly-tracer-databricks/
# MAGIC ├── config/
# MAGIC │   └── settings.py
# MAGIC ├── infrastructure/
# MAGIC │   ├── databricks_client.py
# MAGIC │   ├── databricks_storage.py
# MAGIC │   └── ffmpeg.py
# MAGIC ├── services/
# MAGIC │   ├── camera_config.py
# MAGIC │   ├── databricks_query_service.py
# MAGIC │   └── media_service.py
# MAGIC ├── ui/
# MAGIC │   ├── components.py
# MAGIC │   ├── handlers.py
# MAGIC │   ├── formatters.py
# MAGIC │   └── state.py
# MAGIC ├── camera_config/
# MAGIC │   └── *.yaml
# MAGIC └── databricks_app.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure Databricks Connection
# MAGIC 
# MAGIC Set up authentication and connection parameters

# COMMAND ----------

import os

# Set Databricks connection parameters
# These will be automatically picked up if running in a Databricks notebook
os.environ["DATABRICKS_SERVER_HOSTNAME"] = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_HTTP_PATH"] = "/sql/1.0/warehouses/YOUR_WAREHOUSE_ID"  # Update this!

# Authentication is handled automatically in Databricks notebooks
# The app will use the notebook's context for authentication

print("Configuration:")
print(f"  Server: {os.environ['DATABRICKS_SERVER_HOSTNAME']}")
print(f"  HTTP Path: {os.environ['DATABRICKS_HTTP_PATH']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Launch the Gradio App

# COMMAND ----------

import sys
from pathlib import Path

# Add the application directory to Python path
# Update this path to where you uploaded the app
APP_DIR = "/Workspace/Repos/your-username/anomaly-tracer-databricks"  # Update this!

if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# Import and launch the app
from databricks_app import main

# Launch the app
# The Gradio interface will be displayed in the notebook output
main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alternative: Direct Launch with Inline Code
# MAGIC 
# MAGIC If you prefer to run everything in the notebook without separate files:

# COMMAND ----------

# Uncomment and run this cell for inline launch

# import gradio as gr
# from databricks import sql
# import pandas as pd
# import os
# 
# # Create a simple query function
# def query_data(date_str):
#     connection = sql.connect(
#         server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
#         http_path=os.environ["DATABRICKS_HTTP_PATH"]
#     )
#     
#     query = f"""
#     SELECT * FROM main.cv_logs.gemini_stage1_detections
#     WHERE DATE(processing_timestamp) = '{date_str}'
#     LIMIT 10
#     """
#     
#     with connection.cursor() as cursor:
#         cursor.execute(query)
#         columns = [desc[0] for desc in cursor.description]
#         rows = cursor.fetchall()
#         df = pd.DataFrame(rows, columns=columns)
#     
#     connection.close()
#     return df
# 
# # Create simple Gradio interface
# with gr.Blocks() as demo:
#     gr.Markdown("# CV Inference Traceability Dashboard")
#     date_input = gr.Textbox(label="Date (YYYY-MM-DD)", value="2026-01-21")
#     query_btn = gr.Button("Query")
#     output = gr.Dataframe()
#     
#     query_btn.click(fn=query_data, inputs=date_input, outputs=output)
# 
# demo.launch(share=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC 
# MAGIC ### Connection Issues
# MAGIC - Verify `DATABRICKS_HTTP_PATH` points to a running SQL Warehouse
# MAGIC - Check that the warehouse has access to the Unity Catalog tables
# MAGIC - Ensure you have SELECT permissions on the tables
# MAGIC 
# MAGIC ### Table Access Issues
# MAGIC - Verify table names: `main.cv_logs.gemini_stage1_detections` and `main.cv_logs.stage2_vlm_inferences`
# MAGIC - Check Unity Catalog permissions: `GRANT SELECT ON TABLE main.cv_logs.* TO your_user`
# MAGIC 
# MAGIC ### GCS Access Issues
# MAGIC - The app needs access to GCS for downloading frames and videos
# MAGIC - Configure GCS credentials if running outside Databricks
# MAGIC - For Databricks, ensure the cluster has GCS access configured
# MAGIC 
# MAGIC ### Performance Tips
# MAGIC - Use a larger SQL Warehouse for faster queries
# MAGIC - Consider adding filters (date, farm, camera) to limit result size
# MAGIC - Enable result caching in the SQL Warehouse settings
