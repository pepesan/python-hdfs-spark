# Requiere: docker/01_launch.sh (namenode, datanode) y la entrada
# "127.0.0.1 datanode" en /etc/hosts (ver README.md).
#
# Documentación de acceso a HDFS desde Python3
# https://hdfscli.readthedocs.io/
import hdfs

# Conexión a HDFS
# revisar la configuración de docker/compose.yaml
# 9870 es el puerto HTTP del namenode (WebHDFS), no el RPC (8020)
# "hadoop" es el usuario dueño de /user en el cluster del docker-compose
client = hdfs.InsecureClient('http://localhost:9870', user='hadoop')

"""
InsecureClient(url[, user, ...]) Conexión (sin autenticación) a un namenode vía WebHDFS
client.list(hdfs_path) Listado de una carpeta
client.status(hdfs_path) Información de un fichero/carpeta
client.makedirs(hdfs_path) Crea una carpeta (y las intermedias)
client.upload(hdfs_path, local_path) Sube un fichero/carpeta local a HDFS
client.download(hdfs_path, local_path) Descarga un fichero/carpeta de HDFS
client.delete(hdfs_path, recursive=False) Borra un fichero/carpeta
client.read(hdfs_path) Context manager para leer un fichero (devuelve un file-like)
client.write(hdfs_path) Context manager para escribir un fichero
client.rename(hdfs_src, hdfs_dst) Mueve/renombra un fichero/carpeta
"""

# nos aseguramos de que la carpeta exista (idempotente)
client.makedirs('/user/admin')

# Listado de carpetas
listado = client.list('/user/admin')
print("listado: " + str(listado))
