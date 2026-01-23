#!/usr/bin/env bash
set -e

echo "=== Phase 1 Install Script ==="

sudo apt update
sudo apt install -y python3 python3-venv python3-pip

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install duckdb pandas requests python-dateutil pytz streamlit

mkdir -p db

echo "=== Phase 1 environment ready ==="
