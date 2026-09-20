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

## Entorno Docker (Spark + HDFS + Hive + Zeppelin + Hue)

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
| `hue`           | `gethue/hue@sha256:7d5c1b9f...`               | UI web para explorar HDFS y consultar Hive  |

La imagen de `spark-master`/`spark-worker` (`docker/spark/image/`) extiende
`apache/spark:4.2.0-python3` con un venv `uv` (Python 3.14 + las mismas
librerías que `pyproject.toml`), para que los executors puedan ejecutar
`pandas_udf`/UDFs de Python con las mismas libs que el driver. La imagen de
`zeppelin` (`docker/zeppelin/image/`) reutiliza esa misma imagen (copia
`/opt/spark`, el JDK y el venv) para que Zeppelin hable exactamente el mismo
protocolo Spark que el cluster.

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

### Notebooks de ejemplo de Zeppelin

`docker/zeppelin/ejemplos/` se monta dentro de Zeppelin y se versiona en el
repo:

- `01_introduccion/00_estructura_notebook` — partes de un notebook Zeppelin
  (celdas Markdown/código/resultado), genérico.
- `02_spark_cluster/01_cluster_spark_hive` — `%spark` (Scala) y `%pyspark`
  (con `pandas_udf`, para comprobar que executor y driver comparten
  librerías) contra `spark://spark-master:7077`, `SHOW DATABASES` contra el
  metastore Hive, y `%python` plano.

### Nota: el worker de Spark solo tiene 2 cores/2 GB

El intérprete `%spark` de Zeppelin abre una aplicación Spark persistente
("spark-shared_process") que se queda ocupando recursos del worker mientras
el intérprete siga abierto — con un solo worker de 2 cores, eso puede dejar
sin recursos a cualquier otra aplicación que intente conectarse al mismo
cluster (p. ej. `04_spark_remote.py` desde el host se quedaría colgado
esperando executors). Si pasa esto, reinicia el intérprete de Spark desde
Zeppelin (Interpreter settings → spark → restart) para liberar el worker.

### 1. Crear los volúmenes (bind mounts)

```bash
docker/00_init.sh
```

Crea `docker/volumes/hdfs/{namenode,datanode}` en el host (datos persistentes
de HDFS entre reinicios) con permisos para el usuario del contenedor.

### 2. Levantar el entorno

```bash
docker/01_launch.sh
```

### 3. Ver el estado / los logs

```bash
docker/02_ps.sh
docker/03_logs.sh            # todos los servicios
docker/03_logs.sh namenode   # solo uno
```

### 4. Ejecutar código dentro del cluster

```bash
docker/04_exec_spark.sh                       # shell dentro de spark-master
docker/04_exec_spark.sh 05_spark_sql.py        # spark-submit de un script del proyecto
```

### 5. Parar y limpiar

```bash
docker/20_destroy.sh
```

Para los contenedores (`docker compose down -v`) **y además vacía**
`docker/volumes/hdfs/*`, para poder repetir `00_init.sh` + `01_launch.sh`
desde cero.

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
