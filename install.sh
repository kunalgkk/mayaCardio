#!/usr/bin/env bash
set -e

echo "=== Installing MayaCardio Pipeline on Fresh Board ==="
echo "Target: ARM64 Linux / Raspberry Pi OS"

# Ensure python3 and pip are installed
if ! command -v python3 &> /dev/null; then
    echo "python3 could not be found. Installing via apt..."
    sudo apt-get update && sudo apt-get install -y python3 python3-pip python3-venv
fi

# Create virtual environment if requested/preferred
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment .venv..."
    python3 -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .

chmod +x pipeline
chmod +x install.sh

echo "=== Installation Completed Successfully ==="
echo "Run './pipeline status' to verify."
