#!/usr/bin/env bash
# Muestra los logs del entorno. Sin argumentos, sigue los logs de todos los
# servicios; opcionalmente se puede indicar un servicio concreto, por ejemplo:
#   ./03_logs.sh namenode
set -euo pipefail
cd "$(dirname "$0")"

docker compose logs -f --tail=200 "$@"
