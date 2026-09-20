#!/usr/bin/env bash
# Para los contenedores sin borrarlos (a diferencia de 20_destroy.sh).
# Uso: ./05_stop.sh [servicio ...]  (sin argumentos, para todos)
set -euo pipefail
cd "$(dirname "$0")"

docker compose stop "$@"
