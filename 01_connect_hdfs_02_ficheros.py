# Requiere: docker/01_launch.sh (namenode, datanode) y la entrada
# "127.0.0.1 datanode" en /etc/hosts (ver README.md) — a diferencia de
# 01_connect_hdfs.py (solo listado de metadatos, vía namenode), aquí se
# sube/lee/escribe CONTENIDO de ficheros, y eso exige que el datanode sea
# resoluble desde el host (ver la nota del README sobre el redirect 307).
#
# Ciclo de vida completo de un fichero en HDFS desde Python: subir uno
# local, listar, leer su contenido, borrarlo, y crear/leer/borrar uno
# nuevo directamente en HDFS (sin partir de un fichero local). Ver
# 01_connect_hdfs.py para la lista de métodos del cliente.
import hdfs

client = hdfs.InsecureClient('http://localhost:9870', user='hadoop')
client.makedirs('/user/admin')

# Subir un fichero local a HDFS (podría ser cualquier tipo: txt, csv,
# json... el uso típico es cargar datos de origen para procesarlos luego
# en el cluster, p. ej. una exportación de un CSV).
client.upload('/user/admin/remote-file.txt', './files/local-file.txt', overwrite=True)

filenames = ['/user/admin/' + name for name in client.list('/user/admin')]
if len(filenames) > 0:
    print("Primer fichero: " + filenames[0])
    with client.read(filenames[0]) as f:
        print(f.read())

client.delete('/user/admin/remote-file.txt')

# También se puede escribir un fichero directamente en HDFS, sin que
# exista antes en local — client.write() es un context manager en modo
# binario, igual que open(..., 'wb') pero contra HDFS.
with client.write('/user/admin/myfile.txt', overwrite=True) as f:
    f.write(b'Hello, world!')

with client.read('/user/admin/myfile.txt') as f:
    print(f.read())

client.delete('/user/admin/myfile.txt')
