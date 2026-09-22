#!/usr/bin/env bash
# Levanta el entorno Spark + HDFS definido en compose.yaml.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d

# Tras un arranque en frío (HDFS recién formateado, p. ej. después de
# 20_destroy.sh), "/tmp" en HDFS queda con permisos 755 propiedad de
# "hadoop" — HiveServer2 corre como usuario "hive" y no puede escribir ahí
# (QueryResultsCache.initialize() falla al hacer mkdir), lo que hace que
# hiveserver2 se caiga justo tras iniciar y el contenedor lo reintente cada
# 60s para siempre, nunca llega a escuchar el puerto 10000 (ver CLAUDE.md).
# Idempotente: si ya está bien, chmod/mkdir -p no hacen nada.
echo "Esperando al namenode para fijar permisos HDFS que necesita Hive..."
until docker exec namenode hdfs dfsadmin -report >/dev/null 2>&1; do
    sleep 2
done
docker exec namenode bash -c "hdfs dfs -mkdir -p /tmp /user/hive/warehouse && hdfs dfs -chmod 1777 /tmp && hdfs dfs -chmod -R 1777 /user/hive/warehouse" \
    2>&1 | grep -viE "deprecation|^[0-9]{4}-" || true
echo "Permisos HDFS para Hive aplicados."

docker compose ps
