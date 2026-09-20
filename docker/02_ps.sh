#!/usr/bin/env bash
# Muestra el estado de los contenedores del entorno.
set -euo pipefail
cd "$(dirname "$0")"

docker compose ps
