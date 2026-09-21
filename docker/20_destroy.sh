#!/usr/bin/env bash
# Para y elimina los contenedores/red del entorno, y además deja limpios
# los volúmenes bind mount (datos de HDFS) creados por 00_init.sh, para
# poder arrancar desde cero con 00_init.sh + 01_launch.sh.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v

rm -rf volumes/hdfs/namenode/* volumes/hdfs/namenode/.[!.]* \
       volumes/hdfs/datanode/* volumes/hdfs/datanode/.[!.]* \
       volumes/seaweedfs/* volumes/seaweedfs/.[!.]* \
       volumes/postgres/* volumes/postgres/.[!.]* \
       volumes/kafka/* volumes/kafka/.[!.]* 2>/dev/null || true
rm -f seaweedfs/s3-config/s3.json postgres/.env hue/hue.ini

echo "Contenedores eliminados; volúmenes de docker/volumes/{hdfs,seaweedfs,postgres,kafka}" \
     "vaciados; credenciales S3 de SeaweedFS y contraseña de postgres borradas" \
     "(00_init.sh generará unas nuevas)."
