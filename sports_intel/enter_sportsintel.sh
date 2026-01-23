#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
  echo "ERROR: venv not found in $(pwd). Run install / venv setup first."
  exit 1
fi

# shellcheck disable=SC1091
source venv/bin/activate

echo "Entered SportsIntel."
echo "PWD: $(pwd)"
echo "Python: $(which python)"
