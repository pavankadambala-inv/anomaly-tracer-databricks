# Databricks App Environment Variables

## Required Secrets

Configure these in the Databricks App UI under **Settings → Environment Variables**:

### Databricks OAuth Credentials
- `DATABRICKS_CLIENT_ID` - Your OAuth M2M client ID
- `DATABRICKS_CLIENT_SECRET` - Your OAuth M2M client secret

### GCP Service Account
- `GCP_SERVICE_ACCOUNT_JSON` - Full JSON service account key (single line)

## Non-Secret Environment Variables

These are already configured in `app.yaml`:
- `DATABRICKS_HTTP_PATH` - SQL warehouse HTTP path
- `GRADIO_ROOT_PATH` - Root path for Gradio (empty for Databricks Apps)

## How to Set Secrets in Databricks App UI

1. Go to your Databricks workspace
2. Navigate to **Apps** → Your App
3. Click **Settings** or **Configuration**
4. Go to **Environment Variables** section
5. Add each secret as a key-value pair
6. Restart the app

## Values Reference

See `app.secrets.yaml` in your local development environment for the actual values.
**Never commit `app.secrets.yaml` to Git!**
