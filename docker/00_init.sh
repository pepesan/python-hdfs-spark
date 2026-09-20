#!/usr/bin/env bash
# Crea los volúmenes en formato bind mount (carpetas dentro de docker/volumes)
# que usa compose.yaml para persistir los datos de HDFS entre reinicios.
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p volumes/hdfs/namenode
mkdir -p volumes/hdfs/datanode
mkdir -p volumes/hive/data
mkdir -p volumes/zeppelin/notebook
mkdir -p volumes/zeppelin/logs

# Los contenedores de apache/hadoop, apache/hive y apache/zeppelin corren
# con usuarios distintos al del host (uid 1001, 1000 y 1000
# respectivamente): se abre el permiso para que puedan escribir. Si ya hay
# datos de una ejecución anterior, algunos ficheros son propiedad de esos
# uids y el host no puede hacerles chmod (falla en silencio, no aborta).
chmod -R 777 volumes 2>/dev/null || true

echo "Volúmenes creados en $(pwd)/volumes"
