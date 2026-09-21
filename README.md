# python-hdfs-spark

Ejemplos de PySpark (RDD, DataFrame/SQL, pandas API on Spark, MLlib, GraphX,
Structured Streaming) y de acceso a HDFS desde Python. Incluye un entorno
Docker con un cluster Spark standalone + HDFS para poder ejecutar y comprobar
todos los ejemplos.

## Entorno Python (uv)

El proyecto usa [`uv`](https://docs.astral.sh/uv/) para gestionar el entorno
virtual y las dependencias, con `pyproject.toml` + `uv.lock` como fuente de
verdad.

### 1. Instalar uv

Si no lo tienes instalado todavía:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

(alternativas: `pipx install uv`, o el paquete de tu distro). Comprueba la
instalación con `uv --version`.

### 2. Crear el entorno e instalar las dependencias

Un único comando crea `.venv` (con la versión de Python fijada en
`pyproject.toml` — la más reciente compatible con `pyspark==4.2.0`, que es
Python 3.14) e instala exactamente las versiones ancladas en `uv.lock`:

```bash
uv sync
```

### 3. Ejecutar un ejemplo

```bash
uv run python 03_pyspark_local_02_dataframe.py
```

o, activando el entorno manualmente:

```bash
source .venv/bin/activate
export PYSPARK_PYTHON=python3
python 03_pyspark_local_02_dataframe.py
```

`PYSPARK_PYTHON=python3` es imprescindible: sin él, los workers de Spark en
modo local lanzan el `python3` del sistema (que puede ser una versión
distinta a la del venv) en vez del intérprete del entorno, y scripts que usan
`pandas_udf` / `applyInPandas` (p. ej. `05_spark_sql_06_funciones.py`) fallan
con `PYTHON_VERSION_MISMATCH`. `uv run` no hace falta que lo fijes a mano
porque hereda el `python3` correcto del `.venv` vía `PATH`.

### 4. Añadir/actualizar dependencias

```bash
uv add <paquete>==<version>       # dependencia de ejecución
uv add --dev <paquete>==<version> # dependencia solo de desarrollo (p. ej. pytest)
```

### 5. Tests automatizados

```bash
uv run pytest tests/ -v
```

Valida uno a uno los scripts del proyecto: que se ejecutan sin traceback y,
cuando es posible, que el resultado calculado por Spark es el esperado (no
solo que "no ha fallado"). Los tests que dependen del cluster Docker
(`04_spark_remote.py`, `01_connect_hdfs*.py`) se saltan automáticamente si
`docker/01_launch.sh` no está levantado. Ver `tests/test_examples.py` para el
detalle de qué se comprueba en cada caso.

## Entorno Docker (Spark + HDFS + Hive + Zeppelin + Hue + SeaweedFS + Postgres)

En `docker/` hay un `compose.yaml` que levanta:

| Servicio        | Imagen                                       | Función                                    |
|-----------------|-----------------------------------------------|---------------------------------------------|
| `spark-master`  | `python-hdfs-spark/spark:4.2.0-python3.14` (build propio) | Master de Spark standalone     |
| `spark-worker`  | `python-hdfs-spark/spark:4.2.0-python3.14` (build propio) | Worker de Spark (2 cores, 2g RAM) |
| `namenode`      | `apache/hadoop:3.5.0`                         | NameNode de HDFS                            |
| `datanode`      | `apache/hadoop:3.5.0`                         | DataNode de HDFS                            |
| `hive-metastore`| `apache/hive:4.2.1`                           | Metastore de Hive (Derby embebido)          |
| `hive-server2`  | `apache/hive:4.2.1`                           | HiveServer2 (JDBC/Thrift + UI web)          |
| `zeppelin`      | `python-hdfs-spark/zeppelin:python-3.14` (build propio) | Notebooks (`%spark`, `%pyspark`, `%python`) |
| `hue`           | `gethue/hue@sha256:7d5c1b9f...`               | UI web para explorar HDFS, Hive y Postgres  |
| `seaweedfs`     | `chrislusf/seaweedfs`                         | Servidor S3 (alternativa a HDFS), un solo nodo |
| `postgres`      | `postgres:16-alpine`                          | BBDD relacional de ejemplo (tabla `empleados`) |
| `kafka`         | `apache/kafka:4.3.1`                          | Broker Kafka de un solo nodo (modo KRaft, sin ZooKeeper) |

La imagen de `spark-master`/`spark-worker` (`docker/spark/image/`) extiende
`apache/spark:4.2.0-python3` con un venv `uv` (Python 3.14 + las mismas
librerías que `pyproject.toml`), para que los executors puedan ejecutar
`pandas_udf`/UDFs de Python con las mismas libs que el driver. La imagen de
`zeppelin` (`docker/zeppelin/image/`) reutiliza esa misma imagen (copia
`/opt/spark`, el JDK y el venv) para que Zeppelin hable exactamente el mismo
protocolo Spark que el cluster.

### Uso: arrancar y destruir el entorno

```bash
docker/00_init.sh      # (solo la primera vez, o tras 20_destroy.sh) crea los
                        # volúmenes (docker/volumes/) y genera las credenciales
                        # de SeaweedFS y postgres (y la conexión de Hue a postgres)
docker/01_launch.sh    # levanta TODOS los servicios (docker compose up -d)
docker/02_ps.sh        # ver estado de los contenedores
docker/03_logs.sh [servicio]         # ver logs (todos, o de uno)
docker/04_exec_spark.sh [script.py]  # shell en spark-master, o spark-submit de un script
docker/05_stop.sh [servicio ...]     # parar sin borrar datos
docker/06_start.sh [servicio ...]    # volver a arrancar tras un 05_stop.sh
docker/20_destroy.sh   # BORRA TODO: contenedores + volúmenes + credenciales
                        # generadas (hay que repetir 00_init.sh después)
```

Para arrancar por módulos (recomendado la primera vez, para diagnosticar
capa por capa) en vez de todo de golpe con `01_launch.sh`, ver
`docker/README.md`.

### Puertos publicados en el host

| Servicio       | Puerto | Uso                                                                 |
|----------------|--------|----------------------------------------------------------------------|
| spark-master   | 7077   | RPC del master (`spark://localhost:7077`, usado en `04_spark_remote.py`) |
| spark-master   | 8080   | UI web del master                                                    |
| spark-master   | 4040   | UI web de la aplicación (driver en modo client)                     |
| spark-worker   | 8081   | UI web del worker                                                    |
| namenode       | 8020   | RPC del namenode (clientes HDFS nativos)                             |
| namenode       | 9870   | UI web / **WebHDFS** (usado por el paquete `hdfs` en `01_connect_hdfs*.py`) |
| datanode       | 9864   | UI web / transferencia de datos HTTP (redirección de WebHDFS)        |
| hive-metastore | 9083   | Thrift del metastore (lo usa `hive-server2`, no hace falta abrirlo a mano) |
| hive-server2   | 10000  | JDBC/Thrift (`beeline`, Hue)                                         |
| hive-server2   | 10002  | UI web de HiveServer2                                                |
| zeppelin       | 8082   | UI web de Zeppelin (8080/8081 ya los usa Spark)                     |
| hue            | 8888   | UI web de Hue                                                        |
| seaweedfs      | 9337   | UI/API del master de SeaweedFS (9333 interno, remapeado por si hay otro compose de SeaweedFS levantado en paralelo) |
| seaweedfs      | 8085   | UI/API del volume server (8080 interno, remapeado — 8080 ya lo usa spark-master) |
| seaweedfs      | 8889   | UI/API del filer (8888 interno, remapeado — 8888 ya lo usa Hue)     |
| seaweedfs      | 8333   | **API S3** (usada por `01_connect_s3*.py`)                          |
| postgres       | 5432   | **BBDD relacional** (usada por `01_connect_postgres_07.py` y desde Hue) |
| kafka          | 9092   | **Broker Kafka** (usado por `streaming/02_kafka_productor.py` y `streaming/03_structured_streaming_kafka.py`) |

### Credenciales

Ninguno de los servicios tiene autenticación real (cluster de desarrollo,
sin Kerberos). HDFS usa el "simple/pseudo" auth de Hadoop: cualquier nombre
de usuario vale, se pasa como parámetro (`user.name=...` en WebHDFS, o
`user='hadoop'` en el cliente `hdfs` de Python). El directorio `/user` del
cluster es propiedad de `hadoop`, así que los scripts que escriben en HDFS
deben conectarse como ese usuario.

HiveServer2 tiene `hive.server2.enable.doAs=false` (no hay impersonación:
toda query corre como el usuario `hive` del proceso, el usuario que se pasa
en `beeline -n ...` o en Hue es solo un identificador de sesión). Hue no
trae usuario por defecto: se crea uno la primera vez que se accede a
<http://localhost:8888> (queda como superusuario de esa instancia de Hue).

Las credenciales S3 de SeaweedFS son distintas: `docker/00_init.sh` las
genera aleatoriamente en `docker/seaweedfs/s3-config/s3.json` (identidad
`pepesan`, permisos Admin/Read/Write) y las imprime por pantalla — no están
fijas en ningún fichero versionado. `01_connect_s3_05.py`/
`01_connect_s3_06_ficheros.py` las leen directamente de ese fichero.

La contraseña de `postgres` también la genera `docker/00_init.sh`
(usuario/BBDD fijos: `pyhdfsspark`) y se usa en dos sitios que tienen que
coincidir: `docker/postgres/.env` (el propio contenedor) y
`docker/hue/hue.ini` (la conexión que Hue tiene configurada hacia él,
visible en el desplegable de bases de datos del Editor de Hue) — ninguno
de los dos ficheros reales está en git, solo sus plantillas
(`*.template`). `01_connect_postgres_07.py` la lee directamente de
`docker/postgres/.env`.

### Notebooks de ejemplo de Zeppelin

`docker/zeppelin/ejemplos/` se monta dentro de Zeppelin y se versiona en el
repo, organizado por área de la API de PySpark (igual que los scripts de
la raíz, pero como notebooks — cada uno con celdas `%md` explicando el
contexto antes del código, no solo código suelto):

- `01_introduccion/00_estructura_notebook` — partes de un notebook Zeppelin
  (celdas Markdown/código/resultado), genérico.
- `02_spark_cluster/01_cluster_spark_hive` — `%spark` (Scala) y `%pyspark`
  (con `pandas_udf`, para comprobar que executor y driver comparten
  librerías) contra `spark://spark-master:7077`, `SHOW DATABASES` contra el
  metastore Hive, y `%python` plano.
- `03_conexiones/01_webhdfs_s3_postgres` — HDFS (WebHDFS), S3 (SeaweedFS,
  incluido el ciclo subir/modificar/resubir) y una BBDD relacional
  (PostgreSQL vía JDBC).
- `04_spark_sql/01_seleccion_limpieza_joins_ventanas_fechas` y
  `02_texto_estructuras_pivot_formatos` — recorrido completo de la API de
  DataFrames: selección/agregación, limpieza de datos, joins, window
  functions, fechas, funciones de texto, datos anidados, pivot,
  operaciones de conjuntos, escritura/particionado, Avro y Delta Lake.
- `05_mllib/01_clustering_anomalias_recomendacion` — clusterización
  (K-Means sobre Iris), detección de anomalías (distancia al centro de la
  normalidad) y sistemas de recomendación (ALS, filtrado colaborativo).
  Junto con `06_spark_mllib.py`/`_cancer.py` (clasificación) y
  `06_spark_mllib_boston.py` (regresión) cubre los 5 tipos de problema de
  ML más habituales.
- `06_graph/01_graphframes` — GraphFrames (label propagation, PageRank).
- `07_streaming/01_rate_y_kafka` — Structured Streaming con la fuente
  "rate" y con Kafka real (productor + consumidor).

Los ejemplos que usan un conector externo (GraphFrames/Delta/Kafka/JDBC-
Postgres/Avro) necesitan sus jars añadidos al intérprete `spark`
(`spark.jars.packages` en Interpreter settings) — están persistidos en
`docker/zeppelin/conf-seed/interpreter.json`, no hace falta reaplicarlos.
Detalle de las 3 trampas no obvias al portar estos ejemplos a Zeppelin
(paquete Python que falta en el venv de la imagen, rutas de escritura que
solo el driver ve, Kafka con un solo listener) en `CLAUDE.md`.

**`01_connect_hdfs_04_rpc_nativo_pyarrow.py`, `01_connect_s3_*` (el resto),
`08_spark_final_practice.py` y `label_propagation/` no tienen notebook
equivalente**: el primero no funciona desde Zeppelin por incompatibilidad
de `glibc` (ver más abajo); los demás son variaciones de patrones ya
cubiertos en los notebooks de arriba, o (en el caso de `label_propagation/`)
no usan Spark en absoluto.

### Nota: el worker de Spark solo tiene 2 cores/2 GB

El intérprete `%spark` de Zeppelin abre una aplicación Spark persistente
("spark-shared_process") que se queda ocupando recursos del worker mientras
el intérprete siga abierto — con un solo worker de 2 cores, eso puede dejar
sin recursos a cualquier otra aplicación que intente conectarse al mismo
cluster (p. ej. `04_spark_remote.py` desde el host se quedaría colgado
esperando executors). Si pasa esto, reinicia el intérprete de Spark desde
Zeppelin (Interpreter settings → spark → restart) para liberar el worker.

### Paso manual necesario: resolver "datanode" desde el host

Los clientes HDFS que hablan WebHDFS (como el paquete `hdfs` usado en
`01_connect_hdfs*.py`) primero contactan al namenode (puerto 9870), que para
subir o leer el **contenido** de un fichero los redirige al datanode usando
su hostname interno de Docker (`datanode:9864`). Ese hostname no lo resuelve
tu máquina por defecto, así que hay que añadirlo a `/etc/hosts` una vez
(requiere sudo):

```bash
echo '127.0.0.1 datanode' | sudo tee -a /etc/hosts
```

Sin este paso, operaciones de solo listado/metadatos (`client.list(...)`)
funcionan igualmente, pero `client.upload(...)` / `client.read(...)` /
`client.write(...)` fallan con `NameResolutionError` al intentar conectar a
`datanode`.

## `label_propagation/` — detección de comunidades sin Spark

Detección de comunidades (algoritmo *label propagation*) puro en Python
con `networkx`, sin `pyspark`, sobre un dataset real (red de páginas de
Facebook de políticos, `files/politician_edges.csv`, ~5900 nodos) — sirve
de comparación frente al `labelPropagation` de GraphFrames en
`07_spark_graphx.py` (mismo algoritmo, implementación distinta). A
diferencia del resto de scripts, hay que ejecutarlo desde DENTRO de la
carpeta (rutas relativas propias):

```bash
cd label_propagation
python label_propagation.py
```

## `streaming/` — Structured Streaming

Tres scripts, a ejecutar en orden si se quiere seguir el hilo completo
(o sueltos, el primero no depende de nada):

- **`01_structured_streaming_rate.py`** — introducción a Structured
  Streaming con la fuente "rate" (Spark genera los datos él solo, sin
  nada externo). Ejecutar sin más: `python streaming/01_structured_streaming_rate.py`.
- **`02_kafka_productor.py`** — productor Kafka sencillo (no usa
  `pyspark`), envía unas frases fijas a un topic. Requiere
  `docker/01_launch.sh` (servicio `kafka`).
- **`03_structured_streaming_kafka.py`** — cuenta palabras en tiempo real
  leyendo de Kafka (la fuente de streaming "real" más habitual, a
  diferencia de "rate"). Requiere `kafka` arriba y mensajes en el topic
  (`02_kafka_productor.py` antes o en paralelo) — no termina solo, es
  streaming de verdad (`Ctrl+C` para pararlo).

```bash
docker/01_launch.sh  # si no está ya arriba (necesita el servicio kafka)
cd streaming
python 02_kafka_productor.py
python 03_structured_streaming_kafka.py   # se queda corriendo, Ctrl+C para salir
```

## Datos de ejemplo que hay que descargar aparte

- **`08_spark_final_practice.py`** necesita `files/1500000_Sales_Records.csv`
  (no se sube al repo por tamaño, ~187 MB — está en `.gitignore`). Se
  descarga de <https://excelbianalytics.com/downloads-18-sample-csv-files-data-sets-for-testing-sales/>
  (dataset público de pruebas, sin datos reales) — enlace directo del zip:
  <https://excelbianalytics.com/wp/wp-content/uploads/2017/07/1500000%20Sales%20Records.zip>.
  Descomprimir y colocar el CSV en `files/1500000_Sales_Records.csv`
  (renombrando, el zip trae espacios en el nombre en vez de `_`):
  ```bash
  curl -Lo /tmp/sales.zip "https://excelbianalytics.com/wp/wp-content/uploads/2017/07/1500000%20Sales%20Records.zip"
  unzip -o /tmp/sales.zip -d /tmp/sales
  mv "/tmp/sales/1500000 Sales Records.csv" files/1500000_Sales_Records.csv
  ```
