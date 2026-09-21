# Requiere: docker/01_launch.sh (namenode, datanode, spark-master,
# spark-worker) y ejecutarse DENTRO del contenedor spark-master, no desde
# el host ni desde Zeppelin:
#   ./docker/04_exec_spark.sh 01_connect_hdfs_04_rpc_nativo_pyarrow.py
#
# Por qué solo spark-master/spark-worker: pyarrow.fs.HadoopFileSystem habla
# HDFS por RPC nativo (puerto 8020, protocolo binario Hadoop) en vez de
# WebHDFS (puerto 9870, HTTP — el que usan 01_connect_hdfs*.py con el
# paquete "hdfs"). Para eso usa libhdfs.so, un puente JNI que necesita una
# distribución de Hadoop completa (jars del cliente HDFS + la propia
# librería nativa), no solo los jars hadoop-client-api/runtime que ya trae
# Spark (esos son para el acceso a HDFS de la JVM de Spark, no exponen
# JNI). La imagen python-hdfs-spark/spark:4.2.0-python3.14 la incluye
# recortada (ver docker/spark/image/Dockerfile) junto con las variables de
# entorno necesarias (HADOOP_HOME, ARROW_LIBHDFS_DIR, CLASSPATH).
#
# Por qué NO funciona en Zeppelin (apache/zeppelin:0.12.1, Ubuntu 20.04):
# el libhdfs.so oficial de Hadoop 3.5.0 exige GLIBC >= 2.32, y Ubuntu 20.04
# trae GLIBC 2.31 — a diferencia del ajuste de libstdc++6 (hacia atrás
# compatible, se pudo actualizar vía PPA sin riesgo), actualizar la propia
# glibc del sistema no es seguro. Desde Zeppelin, el acceso a HDFS sigue
# siendo por WebHDFS (ver 01_connect_hdfs.py). Detalle completo en
# CLAUDE.md.
#
# RPC nativo vs WebHDFS: en teoría rinde más para volumen/muchos ficheros
# pequeños (protocolo binario en vez de HTTP+JSON, y permite short-circuit
# read si el cliente corre en el mismo nodo que el datanode) — no medido
# en este proyecto, solo documentado como motivación.
#
# Documentación: https://arrow.apache.org/docs/python/filesystems.html#hadoop-distributed-file-system-hdfs
from pyarrow import fs

# Conexión a HDFS por RPC nativo (puerto 8020, no 9870)
# "namenode" es el hostname interno de docker (ver docker/compose.yaml)
# "hadoop" es el usuario dueño de /user en el cluster del docker-compose
hdfs = fs.HadoopFileSystem('namenode', port=8020, user='hadoop')

# nos aseguramos de que la carpeta exista (idempotente)
hdfs.create_dir('/user/admin')

# Escritura de un fichero
with hdfs.open_output_stream('/user/admin/prueba_pyarrow.txt') as f:
    f.write(b'hola desde pyarrow.fs.HadoopFileSystem (RPC nativo)\n')

# Listado de la carpeta
listado = hdfs.get_file_info(fs.FileSelector('/user/admin'))
print("listado: " + str([info.path for info in listado]))

# Lectura del fichero
with hdfs.open_input_stream('/user/admin/prueba_pyarrow.txt') as f:
    print(f.read())

# borramos el fichero
hdfs.delete_file('/user/admin/prueba_pyarrow.txt')
