#!/usr/bin/env bash
# Vuelve a arrancar contenedores parados con 05_stop.sh.
# Uso: ./06_start.sh [servicio ...]  (sin argumentos, todos)
set -euo pipefail
cd "$(dirname "$0")"

docker compose start "$@"
docker compose ps
