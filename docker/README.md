# Entorno Docker: Spark + HDFS + Hive + Zeppelin + Hue + SeaweedFS

Este `compose.yaml` levanta todo el entorno del proyecto. Los servicios
tienen dependencias reales entre ellos (Hive necesita HDFS, Zeppelin/Hue
necesitan Spark/Hive), así que conviene darlos de alta **por módulos**, de
abajo arriba, en vez de todos a la vez — sobre todo la primera vez, para
poder comprobar cada capa antes de montar la siguiente encima.

Para los puertos, credenciales y notas de compatibilidad de cada servicio,
ver el `README.md` de la raíz del proyecto. Este fichero es solo sobre
**cómo** levantarlo.

## Scripts disponibles

| Script | Qué hace |
|---|---|
| `00_init.sh` | Crea `docker/volumes/` (bind mounts) con los permisos correctos y genera credenciales S3 nuevas para SeaweedFS. Solo hace falta la primera vez, o tras `20_destroy.sh`. |
| `01_launch.sh` | `docker compose up -d` de **todos** los servicios. |
| `02_ps.sh` | Estado de los contenedores. |
| `03_logs.sh [servicio]` | Logs (todos, o de uno). |
| `04_exec_spark.sh [script.py]` | Shell en `spark-master`, o `spark-submit` de un script del proyecto. |
| `05_stop.sh [servicio ...]` | Para contenedores sin borrarlos. |
| `06_start.sh [servicio ...]` | Vuelve a arrancarlos tras un `05_stop.sh`. |
| `20_destroy.sh` | `docker compose down -v`, vacía `docker/volumes/{hdfs,seaweedfs}/*` y borra las credenciales S3 generadas (reset completo). |

Todos aceptan nombres de servicio de `docker compose` como argumento cuando
tiene sentido (`docker compose` por debajo), así que también se puede usar
`docker compose` directamente para cualquier cosa no cubierta por un script.

## Módulos y orden de arranque

### 0. Una sola vez: crear los volúmenes

```bash
./00_init.sh
```

### 1. Módulo base: HDFS

```bash
docker compose up -d namenode datanode
./02_ps.sh
```

Comprobar que el namenode ha arrancado bien (namenode "Started", sin
`NameNode is not formatted` en los logs) antes de seguir:

```bash
./03_logs.sh namenode
curl -s http://localhost:9870/webhdfs/v1/?op=LISTSTATUS   # debería devolver JSON, no un error
```

### 2. Módulo Spark (standalone, master + worker)

`spark-master`/`spark-worker` usan una imagen propia
(`python-hdfs-spark/spark:4.2.0-python3.14`, construida de `./spark/image`)
que añade a `apache/spark:4.2.0-python3` un venv `uv` con las librerías de
`pyproject.toml` — necesario para que `pandas_udf`/UDFs de Python funcionen
en los executors, no solo en el driver. La imagen de Zeppelin (paso 4) la
reutiliza, así que conviene construirla antes:

```bash
docker compose build spark-master   # también sirve para spark-worker, misma imagen
docker compose up -d spark-master spark-worker
./02_ps.sh
```

No depende de HDFS para arrancar (sí para leer/escribir datos ahí, si algún
job lo necesita). Comprobar la UI del master en <http://localhost:8080> —
debe aparecer 1 worker en la sección "Workers".

### 3. Módulo Hive (metastore + HiveServer2)

Necesita HDFS ya arriba (usa `namenode:8020` como warehouse):

```bash
docker compose up -d hive-metastore
./03_logs.sh hive-metastore   # esperar "Starting Hive Metastore Server" sin errores
docker compose up -d hive-server2
./03_logs.sh hive-server2     # esperar a que escuche en el puerto 10000
```

`hive-server2` depende de `hive-metastore` (le habla por Thrift en el puerto
9083), así que si el metastore no ha terminado de inicializar el esquema la
primera vez, `hive-server2` puede tardar en quedar operativo o reintentar la
conexión — revisar sus logs si `beeline`/Hue no consiguen conectar.

### 4. Módulo BBDD: Postgres

Independiente del resto (no depende de HDFS/Spark/Hive) — usado por
`../01_connect_postgres_07.py` (Spark vía JDBC) y registrado como conexión
en Hue (paso 5):

```bash
docker compose up -d postgres
```

La contraseña la genera `00_init.sh` (paso 0) en `docker/postgres/.env` —
si no existe todavía, ejecutar `./00_init.sh` antes de levantar este
servicio. La tabla de ejemplo (`empleados`) la crea
`docker/postgres/init/01_empleados.sql` la primera vez que arranca el
contenedor (volumen de datos vacío) — no se vuelve a ejecutar en arranques
posteriores, ni aunque se cambie el `.sql`.

### 5. Módulo de notebooks/UI: Zeppelin y Hue

Necesitan Spark (Zeppelin, para `%spark`) y Hive (ambos, para consultar
tablas). Hue además necesita Postgres arriba (paso 4) para poder conectar
con él desde el Editor. La imagen de `zeppelin` necesita que
`python-hdfs-spark/spark:...` (paso 2) ya esté construida, porque copia de
ahí Spark/el JDK/el venv:

```bash
docker compose build zeppelin   # solo hace falta la primera vez o si cambia el Dockerfile
docker compose up -d zeppelin hue
```

- Zeppelin: <http://localhost:8082> — el ajuste de versión de Spark
  (`zeppelin.spark.enableSupportedVersionCheck=false`), de bootstrap de
  `%pyspark` (`zeppelin.pyspark.useIPython=false`) y los jars de los
  conectores externos (`spark.jars.packages`: GraphFrames, Delta, Kafka,
  el driver JDBC de Postgres, Avro) están persistidos en
  `docker/volumes/zeppelin/conf/` (sembrado por `00_init.sh` la primera
  vez) — ya no hace falta reaplicarlos tras un `--force-recreate`/rebuild
  (ver `CLAUDE.md` si aun así `%spark`/`%pyspark` fallan al abrir, o si
  falla el `import` de un paquete Python de esos conectores: hay que
  mantener `docker/spark/image/requirements.txt` sincronizado a mano con
  `pyproject.toml`, no se hace solo).
- Hue: <http://localhost:8888> — el primer acceso pide crear un usuario
  (queda como superusuario de esa instancia). La conexión a `postgres` ya
  aparece en el desplegable de bases de datos del Editor (`hue.ini`
  generado por `00_init.sh`, ver README.md de la raíz).

### 6. Módulo S3: SeaweedFS

Independiente del resto (no depende de HDFS/Spark/Hive, es una alternativa
a HDFS para subir datos):

```bash
docker compose up -d seaweedfs
```

Las credenciales S3 las genera `00_init.sh` (paso 0) en
`docker/seaweedfs/s3-config/s3.json` — si no existen todavía, ejecutar
`./00_init.sh` antes de levantar este servicio (el contenedor arranca sin
ese fichero, pero la API S3 lo necesita para autenticar). API S3 en
<http://localhost:8333>, ver `../01_connect_s3_05.py` en la raíz del proyecto.

### 7. Módulo streaming: Kafka

Independiente del resto (no depende de HDFS/Spark/Hive):

```bash
docker compose up -d kafka
```

Un solo broker en modo KRaft (sin ZooKeeper), sin autenticación. Los
topics se crean solos al escribir en ellos (no hace falta crearlos a
mano). Ver `../streaming/02_kafka_productor.py` y
`../streaming/03_structured_streaming_kafka.py` en la raíz del proyecto.

### Levantar todo de una vez

Una vez comprobado que cada módulo funciona por separado, para el día a día
basta con:

```bash
./01_launch.sh
```

(`docker compose up -d` respeta el orden de `depends_on` de `compose.yaml`
automáticamente, así que no hace falta levantar por módulos cada vez — el
arranque por partes de arriba es sobre todo para diagnosticar la primera vez
o tras cambiar la configuración de un servicio concreto).

## Parar / arrancar un módulo suelto

```bash
./05_stop.sh zeppelin hue        # para solo esos dos, sin tocar Spark/HDFS/Hive
./06_start.sh zeppelin hue       # los vuelve a arrancar
```

## Reconstruir las imágenes propias

Si se cambia `spark/image/Dockerfile` o `spark/image/requirements.txt`
(afecta a `spark-master`, `spark-worker` y, de rebote, a `zeppelin`):

```bash
docker compose build spark-master
docker compose build zeppelin       # reconstruye también zeppelin, que copia de la imagen de spark
docker compose up -d --force-recreate spark-master spark-worker zeppelin
```

Si solo cambia `zeppelin/image/Dockerfile`:

```bash
docker compose build zeppelin
docker compose up -d --force-recreate zeppelin
```
