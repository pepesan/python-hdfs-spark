#!/usr/bin/env bash
# Sin argumentos: abre una shell dentro del contenedor spark-master.
# Con un argumento: lo trata como ruta (relativa a la raíz del proyecto,
# montada en /opt/project) de un script Python y lo lanza con spark-submit
# contra el cluster, por ejemplo:
#   ./04_exec_spark.sh 04_spark_remote.py
set -euo pipefail
cd "$(dirname "$0")"

if [ $# -eq 0 ]; then
  docker compose exec spark-master bash
else
  docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    "/opt/project/$1"
fi
