#!/bin/bash
# Start the Python FastAPI backend
cd "$(dirname "$0")"
exec python3.11 -m uvicorn api:app \
  --host 0.0.0.0 \
  --port ${PYTHON_API_PORT:-8001} \
  --reload \
  --reload-dir . \
  --reload-dir ../..
