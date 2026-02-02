#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="$(pwd)/src"
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
