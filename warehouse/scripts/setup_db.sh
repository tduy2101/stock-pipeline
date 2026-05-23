#!/usr/bin/env bash
set -euo pipefail

echo "=== Khoi dong TimescaleDB container ==="
docker compose up -d postgres

echo "=== Cho PostgreSQL san sang ==="
until docker exec stock-pipeline-db pg_isready -U stock -d stock_pipeline; do
  echo "  Waiting..."
  sleep 2
done
sleep 2

echo "=== Apply DDL ==="
docker exec -i stock-pipeline-db psql -U stock -d stock_pipeline < warehouse/ddl/schema.sql

echo "=== DB ready. DATABASE_URL ==="
echo "postgresql://stock:stock@localhost:55432/stock_pipeline"
