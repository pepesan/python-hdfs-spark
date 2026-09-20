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

USER spark
