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

# nos aseguramos de que la carpeta exista (idempotente)
client.makedirs('/user/admin')

# Subida de ficheros
# origen y destino
# se sube para luego trabajar dentro del cluster
# puede ser cualquier tipo de fichero: txt,csv,json...
# estos ficheros son los datos inicales con los que se trabaja
# podría ser una exportación de un excel por ejemplo en csv
# es como hacer un upload a un servidor
client.upload('/user/admin/remote-file.txt', './files/local-file.txt', overwrite=True)

# Coger listado de ficheros
filenames = ['/user/admin/' + name for name in client.list('/user/admin')]
# Cabecera del 1º fichero
if len(filenames) > 0:
    print("Primer fichero: " + filenames[0])
    with client.read(filenames[0]) as f:
        print(f.read())

# borramos el fichero
client.delete('/user/admin/remote-file.txt')

# con esto abrimos un fichero alojado en hdfs
# con permisos de escritura y en binario
# es decir creamos un fichero con un contenido
with client.write('/user/admin/myfile.txt', overwrite=True) as f:
    # una vez abierto el fichero escribimos un contenido
    f.write(b'Hello, world!')
# abrimos un fichero en lectura
with client.read('/user/admin/myfile.txt') as f:
    # leemos el contenido/recorremos el fichero
    print(f.read())

# borramos el fichero
client.delete('/user/admin/myfile.txt')
