#!/usr/bin/env bash
# Para y elimina los contenedores/red del entorno, y además deja limpios
# los volúmenes bind mount (datos de HDFS, postgres, seaweedfs, kafka, hive)
# creados por 00_init.sh, para poder arrancar desde cero con 00_init.sh +
# 01_launch.sh. volumes/zeppelin (notebooks) es intencionalmente persistente
# y no se toca aquí.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v

# Los datos de HDFS/postgres los crean dentro de los contenedores usuarios
# con uid distinto al del host (p. ej. "hadoop" uid 1001, "postgres" uid 70;
# postgres además deja el propio directorio en modo 700, ilegible para el
# usuario del host), así que un rm sin privilegios falla en silencio con
# "Permiso denegado" sobre esos ficheros. Además, el glob (volumes/postgres/*)
# tiene que expandirse YA como root (dentro de "sudo bash -c"), no en el shell
# del usuario normal antes de invocar sudo — si no, como el usuario no puede
# ni listar ese directorio, el glob no expande nada y sudo recibe una ruta
# vacía, borrando "nada" sin avisar.
sudo bash -c '
    rm -rf volumes/hdfs/namenode/* volumes/hdfs/namenode/.[!.]* \
           volumes/hdfs/datanode/* volumes/hdfs/datanode/.[!.]* \
           volumes/seaweedfs/* volumes/seaweedfs/.[!.]* \
           volumes/postgres/* volumes/postgres/.[!.]* \
           volumes/kafka/* volumes/kafka/.[!.]* \
           volumes/hive/data/* volumes/hive/data/.[!.]* 2>/dev/null
' || true
sudo rm -f seaweedfs/s3-config/s3.json postgres/.env hue/hue.ini

echo "Contenedores eliminados; volúmenes de docker/volumes/{hdfs,seaweedfs,postgres,kafka,hive}" \
     "vaciados; credenciales S3 de SeaweedFS y contraseña de postgres borradas" \
     "(00_init.sh generará unas nuevas). volumes/zeppelin no se toca (notebooks persistentes)."
