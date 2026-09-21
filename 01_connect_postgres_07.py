# Requiere: docker/01_launch.sh (servicio postgres) y haber ejecutado
# antes docker/00_init.sh (genera la contraseña en docker/postgres/.env —
# ver README.md).
#
# Conexión a una BBDD relacional (PostgreSQL) desde Spark vía JDBC — a
# diferencia de HDFS/S3 (ficheros), aquí la fuente de datos es una tabla
# de una base de datos, y Spark habla con ella usando el protocolo JDBC
# estándar (el mismo que usaría cualquier cliente SQL: DBeaver, DataGrip,
# beeline...), no un formato de fichero.
#
# Igual que con GraphFrames (07_spark_graphx.py) o Delta Lake
# (08_spark_final_practice.py), el conector JDBC de PostgreSQL es un jar
# Java que no viene con Spark — hay que indicárselo a Spark como
# coordenada Maven ANTES de crear la SparkSession, vía la variable de
# entorno PYSPARK_SUBMIT_ARGS.
#
# Documentación: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
import os
import re

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.7.7 pyspark-shell'

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('conexion-postgres').getOrCreate()

# La contraseña la genera docker/00_init.sh (aleatoria en cada entorno,
# nunca fija en un fichero versionado — ver .gitignore), en formato
# KEY=VALUE tipo .env
with open('docker/postgres/.env') as f:
    contenido = f.read()
password = re.search(r'^POSTGRES_PASSWORD=(.+)$', contenido, re.MULTILINE).group(1)

# revisar la configuración de docker/compose.yaml (servicio "postgres")
# "pyhdfsspark" es el usuario/base de datos que crea el propio contenedor
# la primera vez (variables POSTGRES_USER/POSTGRES_DB)
df = spark.read.format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost:5432/pyhdfsspark') \
    .option('dbtable', 'empleados') \
    .option('user', 'pyhdfsspark') \
    .option('password', password) \
    .option('driver', 'org.postgresql.Driver') \
    .load()

# Mostrar los datos
df.show()
df.printSchema()

# Una vez cargada la tabla como DataFrame, se trabaja con ella exactamente
# igual que con cualquier otra fuente (CSV, JSON, HDFS...): agrupar,
# filtrar, agregar...
resumen = df.groupBy('departamento').agg(
    F.count('*').alias('num_empleados'),
    F.avg('salario').alias('salario_medio'),
)
resumen.orderBy('salario_medio', ascending=False).show()

spark.stop()
