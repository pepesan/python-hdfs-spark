#!/usr/bin/env bash
# Para y elimina los contenedores/red del entorno, y además deja limpios
# los volúmenes bind mount (datos de HDFS) creados por 00_init.sh, para
# poder arrancar desde cero con 00_init.sh + 01_launch.sh.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v

rm -rf volumes/hdfs/namenode/* volumes/hdfs/namenode/.[!.]* \
       volumes/hdfs/datanode/* volumes/hdfs/datanode/.[!.]* 2>/dev/null || true

echo "Contenedores eliminados y volúmenes de docker/volumes/hdfs vaciados."
