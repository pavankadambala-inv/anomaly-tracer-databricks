# CV Inference Traceability Dashboard

A Gradio web application to query and visualize Stage 1 and Stage 2 CV inference results from Databricks Unity Catalog.

## 📁 Project Structure

```
.
├── databricks_app.py          # Main application entry point
├── app.yaml                    # Databricks App configuration
├── app.secrets.yaml           # Local secrets (gitignored)
├── requirements.txt           # Python dependencies
├── run_app.sh                 # Local startup script
├── camera_config/             # Camera configuration files
├── config/                    # Settings and secrets loader
├── infrastructure/            # Databricks & GCP clients
├── services/                  # Query and media services
└── ui/                        # Gradio UI components
```

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Databricks workspace with SQL warehouse
- GCP service account (for frame/video access)

### Local Development

1. **Install dependencies:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure secrets:**
   Create `app.secrets.yaml` with your credentials:
   ```yaml
   DATABRICKS_CLIENT_ID: "your-client-id"
   DATABRICKS_CLIENT_SECRET: "your-client-secret"
   GCP_SERVICE_ACCOUNT_JSON: '{"type":"service_account",...}'
   ```

3. **Run the app:**
   ```bash
   ./run_app.sh
   ```
   
   Or manually:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.databricks.com"
   export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
   python databricks_app.py
   ```

4. **Open in browser:**
   ```
   http://localhost:7860
   ```

## 🔧 Configuration

### Environment Variables
- `DATABRICKS_HOST` - Your Databricks workspace URL
- `DATABRICKS_HTTP_PATH` - SQL warehouse HTTP path
- `DATABRICKS_CLIENT_ID` - OAuth M2M client ID
- `DATABRICKS_CLIENT_SECRET` - OAuth M2M client secret
- `GCP_SERVICE_ACCOUNT_JSON` - GCP service account credentials

### Databricks App Deployment

1. Upload this directory to Databricks Workspace or Repos
2. Create a Databricks App pointing to `databricks_app.py`
3. Set environment variables in the App UI (Settings → Environment Variables)
4. The app will use the values from `app.yaml` for non-secret config

## 📊 Features

- **Date-based filtering** - Query by specific date
- **Time range filtering** - Filter by start/end time
- **Farm/Camera selection** - Filter by farm and camera
- **Stage 1 & 2 results** - View linked inference results
- **Frame viewer** - Animated GIF of detection frames
- **Video player** - Stage 2 classification videos
- **Raw responses** - View full JSON responses from models

## 🔒 Security

- `app.secrets.yaml` is gitignored and never committed
- Credentials are loaded from YAML and set as environment variables
- Databricks SDK handles OAuth M2M authentication automatically

## 📝 License

Internal use only.
