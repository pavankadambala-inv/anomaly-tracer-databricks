# CV Inference Traceability Dashboard

A Gradio web application for querying and visualizing Stage 1 (frame detection) and Stage 2 (video classification) CV inference results from Databricks Unity Catalog.

## Project Structure

```
.
├── databricks_app.py              # Main entry point
├── app.yaml                       # Databricks App configuration
├── app.secrets.yaml               # Local secrets (gitignored)
├── requirements.txt               # Python dependencies
├── config/
│   ├── settings.py                # App settings (tables, catalog, schema)
│   └── secrets_loader.py          # Loads app.secrets.yaml for local dev
├── infrastructure/
│   ├── databricks_client.py       # Databricks SQL connection (OAuth M2M)
│   └── gcs_client.py              # Google Cloud Storage client
├── services/
│   ├── databricks_query_service.py  # SQL queries with auto-reconnect
│   ├── databricks_mapping_service.py # Tenant/farm/camera name mappings
│   └── media_service.py           # GCS media download + GIF creation
├── ui/
│   ├── components.py              # Gradio layout and widgets
│   ├── handlers.py                # UI event handlers
│   ├── formatters.py              # DataFrame formatting for display
│   └── state.py                   # App state and row cache
└── utils/
    └── cleanup.py                 # Temp file LRU cache cleanup
```

## Features

- **Tenant / Farm / Camera filtering** -- cascading dropdowns loaded from Databricks mapping tables
- **Date and time range filtering** -- query by date with optional start/end time
- **Stage 1 & 2 linked results** -- LEFT JOIN on `(camera_id, blk_file, timestamp)`
- **Animated frame viewer** -- GIF built from Stage 1 detection frames (from GCS)
- **Video player** -- Stage 2 classification video playback
- **Raw JSON responses** -- formatted Stage 1 and Stage 2 model outputs
- **Auto-reconnect** -- stale Databricks SQL connections are automatically re-established
- **Row caching** -- prevents redundant media downloads when re-selecting or scrolling

## Databricks Tables

| Table | Catalog.Schema | Purpose |
|-------|---------------|---------|
| `gemini_stage1_detections` | `stg_cv_catalog.bronze` | Stage 1 frame detection results |
| `stage2_vlm_inferences` | `stg_cv_catalog.bronze` | Stage 2 video classification results |
| `tenant_map` | `stg_cv_catalog.bronze` | Tenant ID to name mapping |
| `farm_map` | `stg_cv_catalog.bronze` | Farm ID to name + tenant mapping |
| `farm_camera_map` | `stg_cv_catalog.bronze` | Camera ID to name mapping |

## Important: Mapping Cache Behavior

Tenant, farm, and camera name mappings are loaded **once at app startup** and cached in memory. If the mapping tables are updated (e.g., new farms or cameras added), you must **restart the app** to pick up the changes. The dropdowns for tenant, farm, and camera will continue to show stale names until the app is restarted.

## Databricks App Deployment

1. Push this repo to GitHub
2. Connect the repo to a Databricks App
3. Set the following secrets in the Databricks App UI (Settings > Environment Variables):
   - `DATABRICKS_CLIENT_ID` -- OAuth M2M service principal client ID
   - `DATABRICKS_CLIENT_SECRET` -- OAuth M2M service principal client secret
   - `GCP_SERVICE_ACCOUNT_JSON` -- GCP service account JSON for GCS access
4. `app.yaml` provides non-secret config (`DATABRICKS_HTTP_PATH`, `GRADIO_ROOT_PATH`)

## Local Development

1. **Install dependencies:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Create `app.secrets.yaml`:**
   ```yaml
   DATABRICKS_CLIENT_ID: "your-client-id"
   DATABRICKS_CLIENT_SECRET: "your-client-secret"
   GCP_SERVICE_ACCOUNT_JSON: '{"type":"service_account",...}'
   ```

3. **Run:**
   ```bash
   export DATABRICKS_HOST="https://your-workspace.databricks.com"
   python databricks_app.py
   ```

4. Open `http://localhost:7860`

> The OAuth service principal must have **Can Use** permission on the SQL warehouse for local development to work.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_HOST` | Yes (local) | Workspace URL (auto-set in Databricks Apps) |
| `DATABRICKS_HTTP_PATH` | Yes | SQL warehouse path (set in `app.yaml`) |
| `DATABRICKS_CLIENT_ID` | Yes | OAuth M2M client ID |
| `DATABRICKS_CLIENT_SECRET` | Yes | OAuth M2M client secret |
| `GCP_SERVICE_ACCOUNT_JSON` | Yes | GCP credentials for GCS frame/video access |
| `GRADIO_SERVER_PORT` | No | Override default port 7860 |
| `GRADIO_ROOT_PATH` | No | Reverse proxy path prefix (Databricks Apps) |
