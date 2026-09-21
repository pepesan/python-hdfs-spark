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
mkdir -p volumes/zeppelin/conf
mkdir -p volumes/seaweedfs
mkdir -p volumes/postgres
mkdir -p volumes/kafka

# Los contenedores de apache/hadoop, apache/hive y apache/zeppelin corren
# con usuarios distintos al del host (uid 1001, 1000 y 1000
# respectivamente): se abre el permiso para que puedan escribir. Si ya hay
# datos de una ejecución anterior, algunos ficheros son propiedad de esos
# uids y el host no puede hacerles chmod (falla en silencio, no aborta).
chmod -R 777 volumes 2>/dev/null || true

echo "Volúmenes creados en $(pwd)/volumes"

# Credenciales S3 de SeaweedFS: se generan una vez a partir de la
# plantilla (nunca fijas en un fichero versionado — ver .gitignore) y se
# imprimen por pantalla. Si ya existen, no se tocan.
if [ -f seaweedfs/s3-config/s3.json ]; then
    echo "seaweedfs/s3-config/s3.json ya existe, no se regenera (bórralo si quieres credenciales nuevas)."
else
    access_key=$(openssl rand -hex 10)
    secret_key=$(openssl rand -hex 20)
    sed -e "s/__S3_ACCESS_KEY__/${access_key}/" \
        -e "s/__S3_SECRET_KEY__/${secret_key}/" \
        seaweedfs/s3-config/s3.json.template > seaweedfs/s3-config/s3.json
    echo "Credenciales S3 (SeaweedFS) generadas en seaweedfs/s3-config/s3.json:"
    echo "  accessKey: ${access_key}"
    echo "  secretKey: ${secret_key}"
fi

# conf/ de Zeppelin (interpreter.json en particular): se siembra una sola
# vez desde la plantilla versionada (zeppelin/conf-seed/), que ya trae
# interpreter.json con los 2 ajustes necesarios para Spark 4.2.0
# (zeppelin.spark.enableSupportedVersionCheck=false,
# zeppelin.pyspark.useIPython=false — ver CLAUDE.md). Se copia el
# directorio COMPLETO (no solo interpreter.json: un bind mount de un único
# fichero rompe la escritura atómica de Zeppelin, ver CLAUDE.md). Si ya
# existe (por una ejecución anterior, con la config tal y como quedó en
# Zeppelin tras usarlo), no se toca — evita pisar cambios hechos a mano
# desde la UI.
if [ -f volumes/zeppelin/conf/interpreter.json ]; then
    echo "volumes/zeppelin/conf/ ya existe, no se regenera."
else
    cp zeppelin/conf-seed/* volumes/zeppelin/conf/
    echo "volumes/zeppelin/conf/ sembrado desde zeppelin/conf-seed/."
fi

# Contraseña de Postgres: se genera una vez y se usa en DOS sitios que
# tienen que coincidir — el propio contenedor "postgres" (postgres/.env,
# vía POSTGRES_PASSWORD) y la conexión que Hue tiene configurada hacia él
# (hue/hue.ini, generado también aquí). Ninguno de los dos ficheros reales
# está en git (ver .gitignore), solo sus plantillas. Si ya existe
# postgres/.env, se reutiliza esa misma contraseña (en vez de generar una
# nueva) para que ambos ficheros sigan coincidiendo aunque solo falte uno
# de los dos.
if [ -f postgres/.env ]; then
    postgres_password=$(grep -oP '(?<=^POSTGRES_PASSWORD=).*' postgres/.env)
else
    postgres_password=$(openssl rand -hex 16)
    sed "s/__POSTGRES_PASSWORD__/${postgres_password}/" \
        postgres/.env.template > postgres/.env
    echo "Contraseña de postgres generada en postgres/.env:"
    echo "  usuario: pyhdfsspark"
    echo "  password: ${postgres_password}"
fi

if [ -f hue/hue.ini ]; then
    echo "hue/hue.ini ya existe, no se regenera."
else
    sed "s/__POSTGRES_PASSWORD__/${postgres_password}/" \
        hue/hue.ini.template > hue/hue.ini
    echo "hue/hue.ini generado desde hue/hue.ini.template."
fi
