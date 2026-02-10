#!/bin/bash
# Simple startup script for local testing

set -e

echo "Starting Databricks App..."
echo

# Set required environment variables
export DATABRICKS_HOST="https://4311212186234792.2.gcp.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/1066550024e48b7a"

# Activate virtual environment
source .venv/bin/activate

# Run the app
python databricks_app.py
