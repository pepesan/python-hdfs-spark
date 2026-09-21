# Imagen Spark del proyecto: apache/spark:4.2.0-python3 + un entorno Python
# 3.14 propio (creado con uv) con las mismas librerías que pyproject.toml.
#
# Por qué: PYSPARK_PYTHON debe apuntar a un intérprete que exista en TODOS
# los procesos que ejecutan código Python de Spark, incluidos los executors
# (spark-worker), no solo el driver. Un venv creado en el host o en la
# imagen de Zeppelin no sirve para eso: "uv venv" enlaza bin/python al
# intérprete del sistema en el momento de crearlo (no copia el binario), así
# que el mismo path (p. ej. /opt/project/.venv/bin/python, aunque se monte
# el proyecto en todos los contenedores) resuelve a una versión de Python
# distinta según la imagen base de cada contenedor.
#
# Esta imagen la usan spark-master y spark-worker (mismo entorno en driver y
# executors), y también sirve de base a la imagen de Zeppelin
# (docker/zeppelin/image), que copia /opt/spark, /opt/java/openjdk y
# /opt/venv-project de aquí en vez de reinstalar nada.
FROM apache/spark:4.2.0-python3

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY requirements.txt /tmp/requirements.txt

# uv venv NO copia el binario del intérprete: bin/python queda como symlink
# a donde uv lo haya descargado. Por defecto eso es $HOME/.local/share/uv
# (aquí, /root, porque este RUN es root), un directorio con permisos 700 —
# inaccesible para cualquier usuario que no sea root (p. ej. "spark" o el
# "zeppelin" de la imagen que copia este /opt/venv-project). Forzamos un
# directorio de instalación compartido y con permisos abiertos para que el
# symlink funcione también como usuario sin privilegios.
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python

RUN uv venv --python 3.14 /opt/venv-project && \
    uv pip install --python /opt/venv-project/bin/python --no-cache -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt && \
    chmod -R a+rX /opt/uv && \
    chown -R spark:spark /opt/venv-project

ENV PYSPARK_PYTHON=/opt/venv-project/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv-project/bin/python

# --- HDFS nativo (RPC 8020) para pyarrow.fs.HadoopFileSystem ---
# pyarrow habla HDFS por RPC nativo (puerto 8020, no WebHDFS) vía libhdfs,
# un puente JNI que no trae ni esta imagen ni los jars hadoop-client-*
# que ya usa Spark internamente (esos son para el propio acceso a HDFS de
# la JVM de Spark, no exponen JNI). Hace falta una distribución completa
# de Hadoop; se recorta a lo imprescindible para no disparar el tamaño de
# la imagen: share/hadoop/{common,hdfs} (jars del cliente HDFS) +
# lib/native/libhdfs.so* (las otras libs nativas del tarball — libhdfspp,
# nativetask — pesan >200MB y pyarrow no las usa).
#
# El libhdfs.so oficial de Hadoop 3.5.0 está enlazado contra un libstdc++
# más reciente del que trae Ubuntu 22.04 (falla al cargar con
# "GLIBCXX_3.4.32 not found"). Se actualiza libstdc++6 vía el PPA
# ubuntu-toolchain-r/test — es una biblioteca hacia atrás compatible, no
# rompe nada que ya usara la versión antigua (JVM, numpy, etc.).
RUN apt-get update -qq && \
    apt-get install -y -qq software-properties-common >/dev/null && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update -qq && \
    apt-get install -y -qq --only-upgrade libstdc++6 && \
    apt-get purge -y -qq software-properties-common && \
    apt-get autoremove -y -qq && \
    rm -rf /var/lib/apt/lists/*

ENV HADOOP_VERSION=3.5.0
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
RUN curl -fsSL -o /tmp/hadoop.tar.gz \
      "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar xzf /tmp/hadoop.tar.gz -C /tmp && \
    mkdir -p "${HADOOP_HOME}/share/hadoop" "${HADOOP_HOME}/lib/native" "${HADOOP_HOME}/etc" && \
    cp -r "/tmp/hadoop-${HADOOP_VERSION}/share/hadoop/common" "${HADOOP_HOME}/share/hadoop/" && \
    cp -r "/tmp/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs" "${HADOOP_HOME}/share/hadoop/" && \
    cp -r "/tmp/hadoop-${HADOOP_VERSION}/etc/hadoop" "${HADOOP_HOME}/etc/" && \
    cp -P /tmp/hadoop-${HADOOP_VERSION}/lib/native/libhdfs.so* "${HADOOP_HOME}/lib/native/" && \
    rm -rf /tmp/hadoop.tar.gz "/tmp/hadoop-${HADOOP_VERSION}" \
           "${HADOOP_HOME}"/share/hadoop/common/hadoop-common-*-tests.jar \
           "${HADOOP_HOME}"/share/hadoop/hdfs/hadoop-hdfs-*-tests.jar && \
    chmod -R a+rX "${HADOOP_HOME}"

ENV ARROW_LIBHDFS_DIR=${HADOOP_HOME}/lib/native
# Wildcards "/*" al final: la JVM los expande a todos los .jar del
# directorio al arrancar, no hace falta listar cada jar (más corto y
# resistente a cambios de versión de las dependencias transitivas).
ENV CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"

USER spark
