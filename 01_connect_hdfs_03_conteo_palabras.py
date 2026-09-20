# Requiere: docker/01_launch.sh (namenode, datanode) y la entrada
# "127.0.0.1 datanode" en /etc/hosts (ver README.md).
#
# Documentación de acceso a HDFS desde Python3
# https://hdfscli.readthedocs.io/
import hdfs
from collections import defaultdict, Counter

# Conexión a HDFS
# revisar la configuración de docker/compose.yaml
# 9870 es el puerto HTTP del namenode (WebHDFS), no el RPC (8020)
# "hadoop" es el usuario dueño de /user en el cluster del docker-compose
client = hdfs.InsecureClient('http://localhost:9870', user='hadoop')

# nos aseguramos de que la carpeta exista (idempotente)
client.makedirs('/user/admin')

# Subida de ficheros
client.upload('/user/admin/el_quijote.txt', './files/el_quijote.txt', overwrite=True)
# pillamos el contenido del directorio
filenames = ['/user/admin/' + name for name in client.list('/user/admin')]
# miramos el primer fichero filenames[0]
# head son las primeras lineas
with client.read(filenames[0]) as f:
    print(f.read(200))


# función que cuenta palabras
def count_words(file):
    word_counts = defaultdict(int)
    # leemos línea a línea
    for line in file:
        # dividimos la línea es palabras
        for word in line.split():
            # en un diccionario metemos la palabra como clave
            # y le sumamos 1 al valor
            # en cada entrada tendremos una palabra y cuantas hay en el texto
            word_counts[word] += 1
    return word_counts


# abrimos el fichero desde HDFS
with client.read(filenames[0], encoding='utf-8') as f:
    # lanzamos la función para obtener los valores por palabra
    counts = count_words(f)
    # los imprimimos por pantalla
    print("counts: " + str(counts))

# se ordena de mayor a menor
print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

all_counts = Counter()

for fn in filenames:
    with client.read(fn, encoding='utf-8') as f:
        counts = count_words(f)
        all_counts.update(counts)

print(len(all_counts))
print(sorted(all_counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])
