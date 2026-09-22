# Requiere: docker/01_launch.sh (namenode, datanode) y la entrada
# "127.0.0.1 datanode" en /etc/hosts (ver README.md).
#
# Conexión básica a HDFS desde Python vía WebHDFS (API HTTP del
# namenode), con el paquete `hdfs`. Documentación:
# https://hdfscli.readthedocs.io/
#
# Métodos principales del cliente (los que se usan en este ejemplo y en
# 01_connect_hdfs_02_ficheros.py):
#   client.list(hdfs_path)                   listado de una carpeta
#   client.status(hdfs_path)                 metadatos de un fichero/carpeta
#   client.makedirs(hdfs_path)                crea una carpeta (y las intermedias)
#   client.upload(hdfs_path, local_path)      sube un fichero/carpeta local a HDFS
#   client.download(hdfs_path, local_path)    descarga de HDFS al disco local
#   client.delete(hdfs_path, recursive=False) borra un fichero/carpeta
#   client.read(hdfs_path)                    context manager para leer (file-like)
#   client.write(hdfs_path)                   context manager para escribir
#   client.rename(hdfs_src, hdfs_dst)         mueve/renombra
import hdfs

# 9870 es el puerto HTTP del namenode (WebHDFS), no el RPC (8020, usado
# por clientes nativos como en 01_connect_hdfs_04_rpc_nativo_pyarrow.py).
# "hadoop" es el usuario dueño de /user en el cluster de docker/compose.yaml
# — sin autenticación real (cluster de desarrollo), cualquier nombre de
# usuario vale, pero hay que usar uno con permisos sobre la ruta.
client = hdfs.InsecureClient('http://localhost:9870', user='hadoop')

# makedirs es idempotente: no falla si la carpeta ya existe.
client.makedirs('/user/admin')

listado = client.list('/user/admin')
print("listado: " + str(listado))
