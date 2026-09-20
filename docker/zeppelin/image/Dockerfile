# Imagen Zeppelin del proyecto, con:
# - la MISMA distribución de Spark 4.2.0 (y el mismo JDK) que usan
#   spark-master/spark-worker, y el MISMO venv Python (/opt/venv-project),
#   copiados tal cual de python-hdfs-spark/spark:4.2.0-python3.14 (ver
#   docker/spark/image/Dockerfile) — así el driver (Zeppelin) y los
#   executors (spark-worker) ejecutan exactamente el mismo intérprete con
#   las mismas librerías, algo imprescindible para pandas_udf / UDFs de
#   Python en general. Requiere construir esa imagen antes que esta
#   (`docker compose build spark-master zeppelin`, o `docker compose build`
#   sin más si compose respeta el orden de dependencias del build).
FROM python-hdfs-spark/spark:4.2.0-python3.14 AS spark

FROM apache/zeppelin:0.12.1

USER root

# Spark 4.2.0 exige Java 17+; el JDK 11 que trae la imagen de Zeppelin no
# vale para lanzar el intérprete %spark, así que se sustituye por el mismo
# JDK 21 de la imagen de Spark.
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/venv-project /opt/venv-project
# bin/python de /opt/venv-project es un symlink hacia aquí (ver
# docker/spark/image/Dockerfile) — sin copiar esto también, el symlink
# queda roto en esta imagen.
COPY --from=spark /opt/uv /opt/uv
ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:/opt/venv-project/bin:${PATH}"

RUN mkdir -p /opt/zeppelin/.cache/matplotlib && \
    chown -R 1000:0 /opt/zeppelin/.cache /opt/spark /opt/venv-project

# Tanto %python (zeppelin.python = "python", resuelto por $PATH) como un
# "docker compose exec zeppelin python3" usan este Python 3.14 con las
# librerías del proyecto. PYSPARK_PYTHON asegura que %spark lance los
# workers de Python (driver y executors) con ese mismo intérprete.
ENV PYSPARK_PYTHON=/opt/venv-project/bin/python

USER 1000
