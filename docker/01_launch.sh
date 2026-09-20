#!/usr/bin/env bash
# Levanta el entorno Spark + HDFS definido en compose.yaml.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d
docker compose ps
