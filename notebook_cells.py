# Databricks notebook source
# MAGIC %md
# MAGIC # CV Inference Traceability Dashboard
# MAGIC 
# MAGIC **Your Configuration:**
# MAGIC - Server: `4311212186234792.2.gcp.databricks.com`
# MAGIC - Warehouse: `1066550024e48b7a`
# MAGIC 
# MAGIC **Instructions:**
# MAGIC 1. Run Cell 1 to install dependencies (~2 minutes)
# MAGIC 2. Update APP_DIR in Cell 2 with your email
# MAGIC 3. Run Cell 2 to configure
# MAGIC 4. Run Cell 3 to launch the app
# MAGIC 5. Click the Gradio link to open the dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Install Dependencies (⏱️ ~2 minutes)

# COMMAND ----------

# Install required packages
%pip install gradio>=4.0.0 databricks-sql-connector>=3.0.0 databricks-sdk>=0.18.0 google-cloud-storage>=2.10.0 Pillow>=10.0.0 PyYAML>=6.0

# Restart Python kernel to use new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Configure Application (⏱️ ~5 seconds)
# MAGIC 
# MAGIC **⚠️ IMPORTANT**: Update `APP_DIR` below with your actual email address!

# COMMAND ----------

import os
import sys

# ========================================
# UPDATE THIS PATH WITH YOUR EMAIL!
# ========================================
APP_DIR = "/Workspace/Users/YOUR_EMAIL_HERE/anomaly-tracer"  # ← CHANGE THIS!
# Example: "/Workspace/Users/pavan.kumar@company.com/anomaly-tracer"

# Add to Python path
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# Your Databricks configuration (already filled in!)
os.environ["DATABRICKS_SERVER_HOSTNAME"] = "4311212186234792.2.gcp.databricks.com"
os.environ["DATABRICKS_HTTP_PATH"] = "/sql/1.0/warehouses/1066550024e48b7a"

# Verify configuration
print("=" * 60)
print("Configuration:")
print("=" * 60)
print(f"✓ App Directory: {APP_DIR}")
print(f"✓ Server: {os.environ['DATABRICKS_SERVER_HOSTNAME']}")
print(f"✓ HTTP Path: {os.environ['DATABRICKS_HTTP_PATH']}")
print("=" * 60)

# Verify files exist
if os.path.exists(APP_DIR):
    print(f"✓ App directory exists")
    files = os.listdir(APP_DIR)
    print(f"✓ Found {len(files)} files/folders")
    if "databricks_app.py" in files:
        print("✓ databricks_app.py found")
        print("")
        print("🎉 Configuration looks good! Proceed to Cell 3")
    else:
        print("❌ databricks_app.py NOT found")
        print("   Please check your upload or update APP_DIR path")
else:
    print(f"❌ Directory not found: {APP_DIR}")
    print("   Please update APP_DIR with the correct path")
    print("")
    print("To find your path, run this:")
    print('   %fs ls /Workspace/Users/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Launch Application (⏱️ ~10 seconds)
# MAGIC 
# MAGIC After running this cell, click the Gradio link to open the dashboard.

# COMMAND ----------

# Import and launch the application
from databricks_app import main

# Start the dashboard
print("Starting CV Inference Traceability Dashboard...")
print("")
main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Success!
# MAGIC 
# MAGIC If you see a Gradio link above, click it to open the dashboard.
# MAGIC 
# MAGIC ### Quick Test:
# MAGIC 1. Select a date (e.g., 2026-01-21)
# MAGIC 2. Click "🔄 Load Farms/Cameras"
# MAGIC 3. Click "🔍 Run Query"
# MAGIC 4. Click any row to view frames and video
# MAGIC 
# MAGIC ### Troubleshooting:
# MAGIC 
# MAGIC **If you get an error**, run this test:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Cell (Optional)
# MAGIC 
# MAGIC Run this only if you're having issues:

# COMMAND ----------

# Test Databricks connection
from databricks import sql

print("Testing Databricks SQL connection...")
print("")

try:
    conn = sql.connect(
        server_hostname="4311212186234792.2.gcp.databricks.com",
        http_path="/sql/1.0/warehouses/1066550024e48b7a"
    )
    
    print("✓ Connection successful!")
    print("")
    
    with conn.cursor() as cursor:
        # Test basic query
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"✓ Query execution works: {result}")
        print("")
        
        # Test Stage 1 table
        try:
            cursor.execute("SELECT COUNT(*) FROM main.cv_logs.gemini_stage1_detections LIMIT 1")
            count = cursor.fetchone()[0]
            print(f"✓ Stage 1 table accessible: {count} rows")
        except Exception as e:
            print(f"❌ Stage 1 table error: {e}")
        
        # Test Stage 2 table
        try:
            cursor.execute("SELECT COUNT(*) FROM main.cv_logs.stage2_vlm_inferences LIMIT 1")
            count = cursor.fetchone()[0]
            print(f"✓ Stage 2 table accessible: {count} rows")
        except Exception as e:
            print(f"❌ Stage 2 table error: {e}")
    
    conn.close()
    print("")
    print("All tests passed! ✅")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("")
    print("Possible issues:")
    print("1. SQL Warehouse is not running")
    print("2. You don't have permissions")
    print("3. HTTP path is incorrect")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Help
# MAGIC 
# MAGIC If you're still having issues:
# MAGIC 
# MAGIC 1. **Check SQL Warehouse Status**:
# MAGIC    - Go to SQL Warehouses in the sidebar
# MAGIC    - Find warehouse ID: `1066550024e48b7a`
# MAGIC    - Click "Start" if it's stopped
# MAGIC 
# MAGIC 2. **Check Permissions**:
# MAGIC    ```sql
# MAGIC    GRANT SELECT ON TABLE main.cv_logs.gemini_stage1_detections TO `your-email`;
# MAGIC    GRANT SELECT ON TABLE main.cv_logs.stage2_vlm_inferences TO `your-email`;
# MAGIC    GRANT USAGE ON CATALOG main TO `your-email`;
# MAGIC    GRANT USAGE ON SCHEMA main.cv_logs TO `your-email`;
# MAGIC    ```
# MAGIC 
# MAGIC 3. **Verify Table Names**:
# MAGIC    Run this in SQL Editor:
# MAGIC    ```sql
# MAGIC    SHOW TABLES IN main.cv_logs;
# MAGIC    ```
# MAGIC 
# MAGIC 4. **Check Documentation**:
# MAGIC    - See `YOUR_DEPLOYMENT_GUIDE.md` in the uploaded files
# MAGIC    - See `DATABRICKS_DEPLOYMENT.md` for detailed troubleshooting
