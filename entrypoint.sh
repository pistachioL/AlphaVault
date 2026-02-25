#!/bin/sh
set -eu

echo "[entrypoint] running crawler in background..."
python -u crawler.py &

echo "[entrypoint] starting streamlit on 0.0.0.0:${PORT:-8080}..."
exec streamlit run streamlit_app.py \
  --server.port="${PORT:-8080}" \
  --server.address=0.0.0.0

