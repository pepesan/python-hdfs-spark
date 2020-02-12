# Documentación de acceso a HDFS desde Python3
# https://readthedocs.org/projects/hdfs3/downloads/pdf/latest/
import hdfs3
from collections import defaultdict, Counter

#Conexión a HDFS
# revisar la configuración del docker-compose.yml
# este es el puerto rpc del namenode de hadoop
# por defecto 8020 en la versión 2.7
# pero puede ser otro
hdfs = hdfs3.HDFileSystem('localhost', port=8020)

"""
HDFileSystem([host, port, connect, . . . ]) Connection to an HDFS namenode
HDFileSystem.cat(path) Return contents of file
HDFileSystem.chmod(path, mode) Change access control of given path
HDFileSystem.chown(path, owner, group) Change owner/group
HDFileSystem.df() Used/free disc space on the HDFS system
HDFileSystem.du(path[, total, deep]) Returns file sizes on a path.
HDFileSystem.exists(path) Is there an entry at path?
HDFileSystem.get(hdfs_path, local_path[, . . . ]) Copy HDFS file to local
HDFileSystem.getmerge(path, filename[, . . . ]) Concat all files in path (a directory) to local output file
HDFileSystem.get_block_locations(path[,. . . ]) Fetch physical locations of blocks
HDFileSystem.glob(path) Get list of paths mathing glob-like pattern (i.e., with “*”s).
HDFileSystem.info(path) File information (as a dict)
HDFileSystem.ls(path[, detail]) List files at path
HDFileSystem.mkdir(path) Make directory at path
HDFileSystem.mv(path1, path2) Move file at path1 to path2
HDFileSystem.open(path[, mode, replication, . . . ])
Open a file for reading or writing
HDFileSystem.put(filename, path[, chunk, . . . ]) Copy local file to path in HDFS
HDFileSystem.read_block(fn, offset, length) Read a block of bytes from an HDFS file
HDFileSystem.rm(path[, recursive]) Use recursive for rm -r, i.e., delete directory and contents
HDFileSystem.set_replication(path, replication)
Instruct HDFS to set the replication for the given file.
HDFileSystem.tail(path[, size]) Return last bytes of file
HDFileSystem.touch(path) Create zero-length file
HDFile(fs, path, mode[, replication, buff, . . . ]) File on HDFS
HDFile.close() Flush and close file, ensuring the data is readable
HDFile.flush() Send buffer to the data-node; actual write may happen
later
HDFile.info() Filesystem metadata about this file
HDFile.read([length]) Read bytes from open file
HDFile.readlines() Return all lines in a file as a list
HDFile.seek(offset[, from_what]) Set file read position.
HDFile.tell() Get current byte location in a file
HDFile.write(data) Write bytes to open file (which must be in w or a mode)
HDFSMap(hdfs, root[, check]) Wrap a HDFileSystem as a mutable mapping.
"""

# Listado de carpetas
listado = hdfs.ls('/user/admin')
print("listado: "+str(listado))
#Subida de ficheros
# origen y destino
# se sube para luego trabajar dentro del cluster
# puede ser cualquier tipo de fichero: txt,csv,json...
# estos ficheros son los datos inicales con los que se trabaja
# podría ser una exportación de un excel por ejemplo en csv
# es como hacer un upload a un servidor
put = hdfs.put('./files/local-file.txt', '/user/admin/remote-file.txt')
# Coger listado de ficheros
filenames = []
# glob pilla el contenido del directorio como un array
filenames = hdfs.glob('/user/admin/*')
# Cabecera del 1º fichero
if (len(filenames)>0):
    print("Primer fichero: "+filenames[0])
    print(hdfs.head(filenames[0]))

# borramos el fichero
hdfs.rm('/user/admin/remote-file.txt')

# con esto abrimos un fichero alojado en hdfs
# con permisos de escritura y en binario
# es decir creamos un fichero con un contenido
with hdfs.open('/user/admin/myfile.txt', 'wb') as f:
    # una vez abierto el fichero escribimos un contenido
    f.write(b'Hello, world!')
# abrimos un fichero en lectura, ojo no tiene "wb"
with hdfs.open('/user/admin/myfile.txt') as f:
    #leemos el contenido/recorremos el fichero
    print(f.read())

# borramos el fichero
hdfs.rm('/user/admin/myfile.txt')


#Subida de ficheros
put = hdfs.put('./files/el_quijote.txt', '/user/admin/el_quijote.txt')
# pillamos el contenido del directorio
filenames = hdfs.glob('/user/admin/*')
# miramos el primer fichero filenames[0]
# head son las primeras lineas
print(hdfs.head(filenames[0]))

# función que cuenta palabras
def count_words(file):
    word_counts = defaultdict(int)
    try:
        # leemos línea a línea
        for line in file:
            # dividimos la línea es palabras
            for word in line.split():
                # en un diccionario metemos la palabra como clave
                # y le sumamos 1 al valor
                # en cada entrada tendremos una palabra y cuantas hay en el texto
                word_counts[word] += 1
    #excepción colocada para python 3.7
    except RuntimeError:
        return word_counts
    return word_counts

# abrimos el fichero desde HDFS
with hdfs.open(filenames[0]) as f:
    # lanzamos la función para obtener los valores por palabra
    counts = count_words(f)
    # los imprimimos por pantalla
    print("counts: "+str(counts))

# se ordena de mayor a menor
print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

all_counts = Counter()

for fn in filenames:
    with hdfs.open(fn) as f:
        counts = count_words(f)
        all_counts.update(counts)

print(len(all_counts))
print(sorted(all_counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])
