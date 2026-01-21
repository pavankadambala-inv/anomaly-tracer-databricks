#!/bin/bash
# Setup script for Databricks deployment

set -e  # Exit on error

echo "=========================================="
echo "CV Inference Traceability Dashboard"
echo "Databricks Setup Script"
echo "=========================================="
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed"
    echo "Please install Python 3.8 or later"
    exit 1
fi

echo "✓ Python 3 found: $(python3 --version)"
echo ""

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "ERROR: pip3 is not installed"
    echo "Please install pip3"
    exit 1
fi

echo "✓ pip3 found: $(pip3 --version)"
echo ""

# Create virtual environment (optional but recommended)
read -p "Create a virtual environment? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [ -d "venv" ]; then
        echo "Virtual environment already exists"
    else
        echo "Creating virtual environment..."
        python3 -m venv venv
        echo "✓ Virtual environment created"
    fi
    
    echo "Activating virtual environment..."
    source venv/bin/activate
    echo "✓ Virtual environment activated"
    echo ""
fi

# Install dependencies
echo "Installing Python dependencies..."
pip3 install -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✓ .env file created"
        echo ""
        echo "⚠️  IMPORTANT: Edit .env file with your Databricks credentials"
        echo ""
    else
        echo "WARNING: .env.example not found"
    fi
else
    echo "✓ .env file already exists"
    echo ""
fi

# Prompt for Databricks credentials
echo "=========================================="
echo "Databricks Configuration"
echo "=========================================="
echo ""

read -p "Do you want to configure Databricks credentials now? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p "Databricks Server Hostname (e.g., your-workspace.cloud.databricks.com): " hostname
    read -p "Databricks HTTP Path (e.g., /sql/1.0/warehouses/abc123): " http_path
    read -p "Databricks Token (or press Enter to skip): " token
    
    # Update .env file
    if [ -f ".env" ]; then
        sed -i.bak "s|DATABRICKS_SERVER_HOSTNAME=.*|DATABRICKS_SERVER_HOSTNAME=$hostname|" .env
        sed -i.bak "s|DATABRICKS_HTTP_PATH=.*|DATABRICKS_HTTP_PATH=$http_path|" .env
        if [ ! -z "$token" ]; then
            sed -i.bak "s|DATABRICKS_TOKEN=.*|DATABRICKS_TOKEN=$token|" .env
        fi
        rm -f .env.bak
        echo "✓ Credentials saved to .env"
    fi
    
    # Export for current session
    export DATABRICKS_SERVER_HOSTNAME="$hostname"
    export DATABRICKS_HTTP_PATH="$http_path"
    if [ ! -z "$token" ]; then
        export DATABRICKS_TOKEN="$token"
    fi
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "To run the application:"
echo "  1. Activate virtual environment (if created): source venv/bin/activate"
echo "  2. Set environment variables: source .env"
echo "  3. Run the app: python databricks_app.py"
echo ""
echo "Or run directly:"
echo "  python databricks_app.py"
echo ""
echo "Then open: http://localhost:7860"
echo ""
echo "For Databricks deployment, see: DATABRICKS_DEPLOYMENT.md"
echo ""
